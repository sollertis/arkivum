package EPrints::Plugin::Event::Arkivum;

@ISA = qw( EPrints::Plugin::Event );

use JSON qw(decode_json);
use LWP::UserAgent;
use feature qw{ switch };
use Data::Dumper;


sub new
{
    my( $class, %params ) = @_;
 
    my $self = $class->SUPER::new( %params );
 
    $self->{actions} = [qw( enable disable )];
    $self->{disable} = 0; # always enabled, even in lib/plugins
 
    $self->{package_name} = "Arkivum";

	# Enable debug logging
	$self->__set_debug(1);
 
    return $self;
}


sub astor_checker 
{
	my ( $self ) = @_;

	my $ark_server = $self->param( "server_url" );
	if (not defined $ark_server)
	{
		$self->__log("Arkivum server URL not set-up");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	$self->__log("Running astor_checker to check for astor_status values that need processing...");

   	my $repository = $self->{repository};
	
	# Count the number of records in the astor dataset so we don't try to process 
	# when we don't need too
	my $ds = $repository->dataset( "astor" );

	my $rcount = $ds->count( $repository );
	if ( not defined $rcount )
	{
		$self->__log("astor_checker: Dataset astor not found");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}	

	if ( $rcount == 0)
	{
		return EPrints::Const::HTTP_OK;
	}
	
	# Process documents approved for archive
	$self->__process_requests( "astor", "astor_status", "archive_scheduled", "astor_copy");
	
	# Process documents which are ingesting
	$self->__process_requests( "astor", "astor_status", "ingest_in_progress", "astor_status_checker");

	# Process documents which are replicating
	$self->__process_requests( "astor", "astor_status", "ingested", "astor_status_checker");

	# Process documents which are replicating
	$self->__process_requests( "astor", "astor_status", "replicated", "astor_status_checker");

	# Process documents approved for deletion
	$self->__process_requests( "astor", "astor_status", "delete_scheduled", "astor_delete");

	# Process documents that are being removed by A-Stor deletion
	$self->__process_requests( "astor", "astor_status", "delete_in_progress", "astor_delete_checker");

	$self->__log("Finished astor_checker...");
 
   	return EPrints::Const::HTTP_OK;
}


sub astor_copy
{
	my( $self, $docid, $astorid ) = @_;
	
	# Get the repository
	my $repository = $self->{repository};

	# Get the document we need to copy
	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc )
	{
		$self->__log("Document: $docid not found");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	$self->__log("astor_copy: Copying Document $docid...");

	# Get the storage controller object
	my $storage = $repository->get_storage();
	if ( not defined $storage )
	{
		$self->__log("astor_copy: Could not get the storage controller for Document $docid...");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# Get the specific ArkivumR plugin
	my $plugin = $repository->plugin( "Storage::ArkivumR" );
	if ( not defined $plugin )
	{
		$self->__log("astor_copy: Could not get the A-Stor plugin for Document $docid...");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# Get the status info of the A-Stor server
	my $json = $self->__astor_getStatusInfo();
	if ( not defined $json ) {
		$self->__log("astor_copy: A-Stor service not available..");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# We have contact with the server and have the status
	# so check the free space before we do anything
	my $freespace = $json->{'storage'}{'bytesFree'};
	my $totalsize = 0;

	# We need to check the freespace before we copy the file(s)
	# Over to A-Stor. We should only have one file per document
	# but this may change so we will get and check all files 
	# attached to the document before copying them over.
	foreach my $file (@{$doc->get_value( "files" )})
	{
		my $filesize = $file->value( "filesize" );
		$totalsize = $totalsize + $filesize;
	}
	
	# Now we have the total size in bytes of this copy request
	# we can check the freespace and abort is we don't have
	# enough
	if ( $totalsize > $freespace ) {
		$self->__log("astor_copy: Not enough freespace on A-Stor, copy aborted...");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# Copy all files attached to the document
	foreach my $file (@{$doc->get_value( "files" )})
	{
		# Get the remapped file path so we can find it within A-Stor
		my $filename = $self->__map_to_ark_path($file->get_local_copy());

		# Copy the file to A-Stor
		my $ok = $storage->copy( $plugin, $file);
		if (not $ok) {
			$self->__log("astor_copy: Error copying $filename to A-Stor...");
			return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		}
		$self->__log("astor_copy: Copied $filename to A-Stor for Document $docid...");
	}
	
	# Update the astor record to indicate where we are with the ingest
	$self->__update_astor_record($astorid, "astor_status", "ingest_in_progress");

	$self->__log("astor_copy: Copy Completed for Document $docid...");
	
	return EPrints::Const::HTTP_OK;
}


sub astor_status_checker
{
	my( $self, $docid, $astorid ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		$self->__log("Document $docid not found in astor_status_checker");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	if ( not defined $astor ) 
	{
		$self->__log("Astor record $astorid not found in astor_status_checker");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	$self->__log("astor_status_checker: Checking Document $docid...");
	
	# Get the current state of the astor record of the document
	my $astor_status = $astor->get_value( "astor_status" );

	# We need to check the relevant status for each File attached to the document
	# We will assume that that astor_status will move on only when all files
	# attached to the documeht have reached that state.
	
	my $state_count = 0;
	my $file_count = @{$doc->get_value( "files" )};

	# We need to check all files attached to a document. We then check the A-Stor state
	# for each file and keep a count. If the count equals the number of files attached
	# to the document then we can move the status on.
	foreach my $file (@{$doc->get_value( "files" )})
	{
		# Get the remapped file path so we can find it within A-Stor
		my $filename = $self->__map_to_ark_path($file->get_local_copy());
		
		# Search for the file information so we can extract the state values we need
		my $fileInfo = $self->__astor_getFileInfo($filename);
		if ( not defined $fileInfo )
		{
			$self->__log("astor_status_checker: Error getting file info from A-Stor for Document $docid..");
			return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		}
		
		# Check we have some results before we try to get them. We should have one result
		my $rcount = @{$fileInfo->{"results"}};
		if ( $rcount ne 1 )
		{
			$self->__log("astor_status_checker: No file info returned from A-Stor for Document $docid..");
			return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		}
		
		# Get the ingest and replication status values from A-Stor
		my $ingestState = @{$fileInfo->{"results"}}[0]->{"ingestState"};
		my $replState   = @{$fileInfo->{"results"}}[0]->{"replicationState"};
		my $astorMD5	= @{$fileInfo->{"results"}}[0]->{"MD5checksum"};
		
		given ($astor_status) {
		  when(/^ingest_in_progress/) {
			if ( $ingestState eq "FINAL" ) {

				# We should check the MD5 Checksum of the file in both EPrints and A-Stor 
				# to ensure they are the same. If they are not then we report this and 
				# stop the copy process
				my $eprintsMD5 = $file->generate_md5();
				if ( $eprintsMD5 ne $astorMD5 ) {
					$self->__log("astor_status_checker: File checksum in eprints does not match A-Stor for $filename in document $docid..");
					$self->__update_astor_record($astorid, "astor_status", "archive_failed");
					return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
				}
				$state_count = $state_count + 1;
			}
		  }
		  when(/^ingested/) { 
			if ( $replState eq "amber" ) {
				$state_count = $state_count + 1;
			}

			# We may have replicated already so fix the status value
			if ( $replState eq "green" ) {
				$astor_status = "replicated";
				$state_count = $state_count + 1;
			}
		  }
		  when(/^replicated/) { 
			if ( $replState eq "green" ) {
				$state_count = $state_count + 1;
			}
		  }
		}
	}

	# If all files have the required state then we can change the astor_status
	if ( $state_count == $file_count) {
		given ($astor_status) {
		  when(/^ingest_in_progress/) {
			$self->__update_astor_record($astorid, "astor_status", "ingested");
		  }
		  when(/^ingested/) { 
			$self->__update_astor_record($astorid, "astor_status", "replicated");
		  }
		  when(/^replicated/) { 
			$self->__update_astor_record($astorid, "astor_status", "escrow");
			$self->__update_document_record($docid, "archive_status", "archived");
		  }
		  when(/^delete_in_progress/) {
			$self->__update_astor_record($astorid, "astor_status", "deleted");
			$self->__update_document_record($docid, "archive_status", "deleted");
		  }
		}
	}

	$self->__log("astor_status_checker: Checking completed for Document $docid...");

	return EPrints::Const::HTTP_OK;
}


sub astor_delete
{
	my( $self, $docid, $astorid ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		$self->__log("astor_delete: Document $docid not found...");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	$self->__log("astor_copy: Deleting Document $docid from A-Stor...");

	# Get the storage controller object
	my $storage = $repository->get_storage();
	if ( not defined $storage )
	{
		$self->__log("astor_delete: Could not get the storage controller for Document $docid...");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# Get the specific ArkivumR plugin
	my $plugin = $repository->plugin( "Storage::ArkivumR" );
	if ( not defined $plugin )
	{
		$self->__log("astor_delete: Could not get the A-Stor plugin for Document $docid...");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# Get the status info of the A-Stor server
	my $json = $self->__astor_getStatusInfo();
	if ( not defined $json ) {
		$self->__log("astor_delete: A-Stor service not available..");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	# Process all files attached to the document
	foreach my $file (@{$doc->get_value( "files" )})
	{
		# Delete the file stored on the A-Stor plugin
		my $filename = $self->__map_to_ark_path($file->get_local_copy());

		my $response = $self->__astor_deleteFile($filename);
		if ($response->is_error) 
		{
			$self->__log("astor_delete: Error invalid response returned: " . $response->status_line);
			return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		}

#		my $ok = $storage->delete_copy( $plugin, $file);
#		if (not $ok) {
#			$self->__log("astor_delete: Error deleting $filename from A-Stor...");
#			return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
#		}

		$self->__log("astor_delete: Deleted $filename from A-Stor for Document $docid...");
	}

	# Update the A-Stor record to indicate where we are with the ingest
	$self->__update_astor_record($astorid, "astor_status", "delete_in_progress");

	$self->__log("astor_delete: Delete completed for Document $docid...");

	return EPrints::Const::HTTP_OK;
}



sub astor_delete_checker
{
	my( $self, $docid, $astorid ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		$self->__log("astor_delete_checker: Document $docid not found");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	if ( not defined $astor ) 
	{
		$self->__log("astor_delete_checker: Astor record $astorid not found");
		return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	}

	$self->__log("astor_delete_checker: Checking Document $docid...");

	# Get the current state of the astor record of the document
	my $astor_status = $astor->get_value( "astor_status" );

	# We need to check that each file of the document object has been removed.
	# Once they've all gone then we can move the status on.
	
	my $state_count = 0;

	# Process each file attached to the eprints document
	foreach my $file (@{$doc->get_value( "files" )})
	{
		# Get the remapped file path so we can find it within A-Stor
		my $filename = $self->__map_to_ark_path($file->get_local_copy());
		
		# Search for the file information so we can extract the state values we need
		my $fileInfo = $self->__astor_getFileInfo($filename);
		if ( not defined $fileInfo )
		{
			$self->__log("astor_delete_checker: Error getting file info from A-Stor for Document $docid..");
			return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		}
		
		# Check if any files have been returned in the results. We are expecting
		# zero, so if state_count for all files is not zeor then we can't 
		# change the status
		my $rcount = @{$fileInfo->{"results"}};
		$state_count = $state_count + $rcount;
	}
	
	# Check to see if we had any files left in A-Stor, if not then we can change the status
	if ( $state_count == 0) {
		if ( $astor_status eq "delete_in_progress" ) 
		{
			$self->__update_astor_record($astorid, "astor_status", "deleted");
			$self->__update_document_record($docid, "archive_status", "deleted");
		}
	}

	$self->__log("astor_delete_checker: Checking completed for Document $docid...");

	return EPrints::Const::HTTP_OK;
}


sub __process_requests
{
	my( $self, $dataset, $key, $value, $action ) = @_;

	my $repository = $self->{repository};
	
	my $ds = $repository->dataset( $dataset );

	$self->__log("__process_requests: Searching for $key values $value...");

	# Create search expression
	my $search = new EPrints::Search( session=>$repository, dataset=>$ds );

	# Add filter
	$search->add_field( $ds->get_field( $key ), $value, "EQ", "ALL" );
 
	# Perform the search
	my $results = $search->perform_search;

	# Get all matching ids
	my $ids = $results->get_ids; 
 
	$results->map( 
		sub {
			my( $session, $dataset, $doc ) = @_;

			my $docid = $doc->get_value("docid");
			$self->__log("__process_requests: Creating $action task for Document $docid...");

			# Create an Event Task to process the copy action for this EPrint
			$repository->dataset( "event_queue" )->create_dataobj({
				pluginid => "Event::Arkivum",
				action => $action,
				params => [$doc->get_value("docid"), $doc->get_value("astorid")], });
		}
	);
 
	$results->dispose;

	$self->__log("__process_requests: Finished searching for $key values $value...");
	
	return;
}


sub __update_astor_record
{
	my( $self, $astorid, $key, $value ) = @_;

	my $repository = $self->{repository};

	my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	if ( not defined $astor ) 
	{
		$self->__log("Astor record $astorid not found in __update_astor_record");
		return 0;
	}
	
	# We have a record so update the field value
	$astor->set_value($key, $value);
	return $astor->commit();
}


sub __update_document_record
{
	my( $self, $docid, $key, $value ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		$self->__log("Document $docid not found in __update_documenht_record");
		return 0;
	}
	
	# We have a record so update the field value
	$doc->set_value($key, $value);
	return $doc->commit();
}


#
#	A-Stor REST API Functions
#


sub __astor_getStatusInfo 
{
	my( $self ) = @_;

	my $api_url = "/json/status/info/";
	
	my $response = $self->__astor_getRequest($api_url);
	if ( not defined $response )
	{
		$self->__log("Inavlid response returned in __astor_getStatus");
		return;
	}

	if ($response->is_error) 
	{
		$self->__log("Invalid response returned in __astor_getStatus: $response->status_line");
		return;
	}
  
	# Get the content which should be a json string
	my $json = decode_json($response->content);
	if ( not defined $json) {
		$self->__log("Invalid response returned in __astor_getStatus");
		return;
	}
  
	return $json;
}


sub __astor_getFileInfo
{
	my( $self, $filename) = @_;

	my $api_url = "/json/search/files?path=" . $filename;

	my $response = $self->__astor_getRequest($api_url);
	if ( not defined $response )
	{
		$self->__log("__astor_getFileInfo: Invalid response returned...");
		return;
	}

	if ($response->is_error) 
	{
		$self->__log("__astor_getFileInfo: Invalid response returned: $response->status_line");
		return;
	}

	# Get the content which should be a json string
	my $json = decode_json($response->content);
	if ( not defined $json) {
		$self->__log("__astor_getFileInfo: Invalid response returned...");
		return;
	}
  
	return $json;
}


sub __astor_deleteFile
{
	my( $self, $filename) = @_;

	my $api_url = "/files" . $filename;
	
	my $response = $self->__astor_deleteRequest($api_url);
	if ( not defined $response )
	{
		$self->__log("__astor_getFileInfo: Invalid response returned...");
		return;
	}
  
	return $response;
}


sub __astor_getRequest 
{
	my( $self, $url, $params ) = @_;

	my $ark_server = $self->param( "server_url" );
	my $server_url = $ark_server . $url;

	my $ua       = LWP::UserAgent->new();
	my $response = $ua->get( $server_url, $params );
  
	return $response;
}


sub __astor_deleteRequest 
{
	my( $self, $url, $params) = @_;

	my $ark_server = $self->param( "server_url" );
	my $server_url = $ark_server . $url;
	
	my $ua       = LWP::UserAgent->new();
	my $req = HTTP::Request->new(DELETE => $server_url);
	my $response = $ua->request($req);
  
	return $response;
}


sub __log 
{
	my ( $self, $msg) = @_;

	$self->{repository}->log($msg);
}


sub __set_debug 
{
	my ( $self, $enabled) = @_;
	
	my $repo = $self->{repository};
	if ( $enabled ) {
		$repo->{noise} = 1;
	} else {
		$repo->{noise} = 0;
	}
}


sub __map_to_ark_path 
{
	
	my( $self, $local_path) = @_;
	
	# Get the root path for repository as it would be on local storage
	my $repo = $self->{session}->get_repository;
	my $local_root_path = $repo->get_conf( "archiveroot" );

	# Get the configured mount path for the Arkivum storage and append the repo id to it
	my $ark_mount_path = $self->param( "mount_path" );
	$ark_mount_path .= '/'.$repo->id;
	
	# Normalize to make sure there are no double-slashes in the mount path
	$ark_mount_path =~ s#//#/#g;
	
	# Replace the local root path with the Arkivum mount path
	my $mapped_path = $local_path;
	$mapped_path =~ s#$local_root_path#$ark_mount_path#;
	
	return $mapped_path;
}

1;