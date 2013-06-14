package EPrints::Plugin::Event::Arkivum;

@ISA = qw( EPrints::Plugin::Event );

use JSON qw(decode_json);
use LWP::UserAgent;
use feature qw{ switch };

sub new
{
    my( $class, %params ) = @_;
 
    my $self = $class->SUPER::new( %params );
 
    $self->{actions} = [qw( enable disable )];
    $self->{disable} = 0; # always enabled, even in lib/plugins
 
    $self->{package_name} = "Arkivum";
 
    return $self;
}

sub astor_checker 
{
	my ( $self ) = @_;

	if (not defined $ark_server)
	{
		print "Arkivum server URL not set\n";
	}

   	my $repository = $self->{repository};
	
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
 
   	return;
}

sub __process_requests
{
	my( $self, $dataset, $key, $value, $action ) = @_;

	my $repository = $self->{repository};
	
	my $ds = $repository->dataset( $dataset );

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

			# Create an Event Task to process the copy action for this EPrint
			$repository->dataset( "event_queue" )->create_dataobj({
				pluginid => "Event::Arkivum",
				action => $action,
				params => [$doc->get_value("docid"), $doc->get_value("astorid")], });
		}
	);
 
	$results->dispose;
	
	return;
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
		print "Document: $docid not found\n";
		return;
	}

	# Get the storage controller object
	my $storage = $repository->get_storage();
	exit( 0 ) unless( defined $storage );

	# Get the zpecific ArkivumR plugin
	my $plugin = $repository->plugin( "Storage::ArkivumR" );
	exit( 0 ) unless( defined $plugin );

	# Get the status info of the A-Stor server
	my $json = $self->__astor_getStatusInfo();
	if ( not defined $json ) {
		print "A-Stor service not available\n";
		return;
	}

	# We have contact with the server and have the statuxs
	# so check the free space before we do anything
	my $freespace = $json->{'storage'}{'bytesFree'};
	print "bytesfree: $freespace\n";
	
	# Process all files attached to the document
	foreach my $file (@{$doc->get_value( "files" )})
	{
		print "\t" . $file->get_local_copy() . "\n";
		
		# Copy the file to the Arkivum Storage
		my $ok = $storage->copy( $plugin, $file);
		if (not $ok) {
			print "\tError ccopy file to Arkivum Storage..\n";
		}
	}
	
	# Update the astor record to indicate where we are with the ingest
	$self->__update_astor_record($astorid, "astor_status", "ingest_in_progress");
	
	return;
}


sub astor_status_checker
{
	my( $self, $docid, $astorid ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		print "Document: $docid not found\n";
		return;
	}

	my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	if ( not defined $astor ) 
	{
		print "Document: $astorid not found\n";
		return;
	}
	
	# Get the current state
	my $astor_status = $astor->get_value( "astor_status" );

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

	return;
}


sub astor_delete
{
	my( $self, $docid, $astorid ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		print "Document: $docid not found\n";
		return;
	}
	
	# Update the astor record to indicate where we are with the ingest
	$self->__update_astor_record($astorid, "astor_status", "delete_in_progress");

	return;
}


sub __update_astor_record
{
	my( $self, $astorid, $key, $value ) = @_;

	my $repository = $self->{repository};

	my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	if ( not defined $astor ) 
	{
		print "__update_astor_record: $astorid not found\n";
		return;
	}
	
	# We have a record so update the field value
	$astor->set_value($key, $value);
	$astor->commit();
}


sub __update_document_record
{
	my( $self, $docid, $key, $value ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	if ( not defined $doc ) 
	{
		print "Error in __update_astor_record: $docid not found\n";
		return;
	}
	
	# We have a record so update the field value
	$doc->set_value($key, $value);
	$doc->commit();
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
		print "Error: __astor_getRequest did not return a valid response\n";
		return;
	}

	if ($response->is_error) 
	{
		print "\tFailed. Error: " . $response->status_line . "\n";
		return;
	}
  
	# Get the content which should be a json string
	my $json = decode_json($response->content);
	if ( not defined $json) {
		print "Error: __astor_getStatusInfo failed to get a valid response from server.\n";
		return;
	}
  
	return $json;
}


sub __astor_getRequest 
{
	my( $self, $url, $params ) = @_;

	my $ark_server = $self->param( "server_url" );
	my $server_url = $ark_server . $url;

	print "get_url: $server_url\n";

	my $ua       = LWP::UserAgent->new();
	my $response = $ua->get( $server_url, $params );
  
	return $response;
}


sub __astor_postRequest 
{
	my( $self, $url, $params ) = @_;

	my $ark_server = $self->param( "server_url" );
	my $server_url = $ark_server . $url;

	my $ua       = LWP::UserAgent->new();
	my $response = $ua->post( $server_url, $params );
  
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

1;