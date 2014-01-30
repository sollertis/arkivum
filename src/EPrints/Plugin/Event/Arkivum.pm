=head1 NAME

EPrints::Plugin::Event::Arkivum - Event task to manage archiving to A-Stor service

=head1 SYNOPSIS

	# cfg.d/x_arkivum.pl
	$c->{plugins}->{"Event::Arkivum"}->{params}->{server_url} = "https://...";

=head1 DESCRIPTION

See L<EPrints::Plugin::Event> for available methods.

To enable this module you must specify the server url for the Arkivum appliance.

=head1 METHODS

=cut

package EPrints::Plugin::Event::Arkivum;

@ISA = qw( EPrints::Plugin::Event );

use strict;

use JSON qw(decode_json);
use LWP::UserAgent;


sub new
{
    my( $class, %params ) = @_;
 
    my $self = $class->SUPER::new( %params );
 
    $self->{actions} = [qw( enable disable )];
    $self->{disable} = 0; # always enabled, even in lib/plugins
 
    $self->{package_name} = "Arkivum";

	  # Enable debug logging
	  $self->_set_debug(1);
 
    return $self;
}


######################################################################

=over 4

=item astor_checker

Check the if any archive or delete requests for ePrints documents have
been made and create the appropriate event task for each specific docid.
This event will also check the status of any copy or delete event that are in 
progress.

This event is run according to a cron event configured in the control screen for the
plugin. By default it will run every 15 minutes.

If this event succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_checker 
{
    my ( $self ) = @_;

	  $self->_log("Starting astor_checker...");

	  my $ark_server = $self->param( "server_url" );
	  if (not defined $ark_server)
	  {
		  $self->_log("Arkivum server URL not set-up");
		  return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

   	my $repository = $self->{repository};
	
	  # First process any astor document requests but check to see if we have any 
	  my $ds = $repository->dataset( "astor" );

	  my $rcount = $ds->count( $repository );
	  if ( not defined $rcount )
	  {
		    $self->_log("astor_checker: Dataset astor not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }	

	  # If we have some astor document request then process them based on the archive_status value
	  if ( $rcount != 0)
	  {
		    $self->_log("Found some astor records to check...");
		    $self->_process_doc_requests( "astor", "astor_status", "archive_scheduled", "astor_doc_copy");
		    $self->_process_doc_requests( "astor", "astor_status", "ingest_in_progress", "astor_doc_status_checker");
		    $self->_process_doc_requests( "astor", "astor_status", "ingested", "astor_doc_status_checker");
		    $self->_process_doc_requests( "astor", "astor_status", "replicated", "astor_doc_status_checker");
		    $self->_process_doc_requests( "astor", "astor_status", "delete_scheduled", "astor_doc_delete");
		    $self->_process_doc_requests( "astor", "astor_status", "delete_in_progress", "astor_doc_delete_checker");
		    $self->_process_doc_requests( "astor", "astor_status", "restore_scheduled", "astor_doc_restore_request");
		    $self->_process_doc_requests( "astor", "astor_status", "restore_in_progress", "astor_doc_restore_checker");
	  }
	  else
	  {
		    $self->_log("No astor records to process..");
	  }

	  # Now process any astor eprint requests after checking if we have any
	  $ds = $repository->dataset( "astor_eprint" );

	  $rcount = $ds->count( $repository );
	  if ( not defined $rcount )
	  {
		    $self->_log("astor_checker: Dataset astor_eprint not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }	

	  if ( $rcount != 0)
	  {
		    $self->_log("astor_checker: Processing astor_eprint requests");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "archive_scheduled", "astor_eprint_archive_request");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "ingest_in_progress", "astor_eprint_archive_checker");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "ingested", "astor_eprint_archive_checker");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "replicated", "astor_eprint_archive_checker");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "restore_scheduled", "astor_eprint_restore_request");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "restore_in_progress", "astor_eprint_restore_checker");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "delete_scheduled", "astor_eprint_delete_request");
		    $self->_process_eprint_requests( "astor_eprint", "astor_status", "delete_in_progress", "astor_eprint_delete_checker");
	  }
	  else 
	  {
		    $self->_log("No astor_eprint records to process..");
	  }
	
	  $self->_log("Finished astor_checker...");
   	return EPrints::Const::HTTP_OK;
}

######################################################################

=over 4

=item astor_eprint_archive_request

Process a request to archive an eprint and all of its documents and files.

This task will check the eprint and then iterate over all of tis documents
and generate a document request for each one. The task will be complete
when all of the document tasks have been completed either through a success
or a failure.

=back

=cut

#####################################################################

sub astor_eprint_archive_request
{
	  my( $self, $eprintid, $astorid ) = @_;

	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Iterate through all of the document objects and make a request
	  foreach my $doc ($eprint->get_all_documents())
	  {
		    my $docid = $doc->get_value("docid");

		    # Process documents approved for archive
		    my $astor = $repository->dataset( "astor" )->create_dataobj({
			    userid => $astor->get_value("userid"),
			    docid => $docid,
			    parentid => $astorid,
			    justification => 'EPrint A-Stor Archive Request for ' . $eprintid,
			    astor_status => 'archive_scheduled',
		    });
		
		    # Check we've created the document request record and fail if we haven't
		    if ( not defined $astor )
		    {
			    $self->_log("astor_eprint_archive_request: Error creating document request for $eprintid");
			    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		    }
	  }
	
	  # Update the astor record to indicate where we are with the ingest
	  $self->_update_astor_eprint_record($astorid, "astor_status", "ingest_in_progress");
	
	  # Finally update the eprint archive_status to reflect that its documents have been
	  # scheduled.
	  $eprint->set_value("archive_status","archive_in_progress");
	  $eprint->commit();
	
	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_eprint_restore_request

Process a request to restore an eprint and all of its documents and files 
from the A-Stor service to the local A-Stor appliance

This task will check the eprint and then iterate over all of tis documents
and generate a document request for each one. The task will be complete
when all of the document tasks have been completed either through a success
or a failure.

=back

=cut

#####################################################################

sub astor_eprint_restore_request
{
	  my( $self, $eprintid, $astorid ) = @_;

	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Iterate through all of the document objects and make a request
	  foreach my $doc ($eprint->get_all_documents())
	  {
		    my $docid = $doc->get_value("docid");

		    # Process documents approved for archive
		    my $astor = $repository->dataset( "astor" )->create_dataobj({
			      userid => $astor->get_value("userid"),
			      docid => $docid,
			      parentid => $astorid,
			      justification => 'EPrint A-Stor Restore Request for ' . $eprintid,
			      astor_status => 'restore_scheduled',
		    });
		
		    # Check we've created the document request record and fail if we haven't
		    if ( not defined $astor )
		    {
			      $self->_log("astor_eprint_restore_request: Error creating document request for $eprintid");
			      return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		    }
	  }

	# Update the astor record to indicate where we are with the ingest
	$self->_update_astor_eprint_record($astorid, "astor_status", "restore_in_progress");
	
	return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_eprint_delete_request

Process a request to delete an eprint and all of its documents and files 
from the A-Stor service

This task will check the eprint and then iterate over all of tis documents
and generate a document request for each one. The task will be complete
when all of the document tasks have been completed either through a success
or a failure.

=back

=cut

#####################################################################

sub astor_eprint_delete_request
{
	  my( $self, $eprintid, $astorid ) = @_;

	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Iterate through all of the document objects and make a request
	  foreach my $doc ($eprint->get_all_documents())
	  {
		    my $docid = $doc->get_value("docid");

		    # Process documents approved for archive
		    my $astor = $repository->dataset( "astor" )->create_dataobj({
			      userid => $astor->get_value("userid"),
			      docid => $docid,
			      parentid => $astorid,
			      justification => 'EPrint A-Stor Restore Request for ' . $eprintid,
			      astor_status => 'delete_scheduled',
		    });
		
		    # Check we've created the document request record and fail if we haven't
		    if ( not defined $astor )
		    {
			      $self->_log("astor_eprint_delete_request: Error creating document request for $eprintid");
			      return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		    }
	  }

	# Update the astor record to indicate where we are with the ingest
	$self->_update_astor_eprint_record($astorid, "astor_status", "delete_in_progress");
	
	return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_eprint_archive_checker

Check the status of a restore request 

=back

=cut

#####################################################################

sub astor_eprint_archive_checker
{
	  my( $self, $eprintid, $astorid ) = @_;

	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $scheduled_count		= 0;
	  my $inprogress_count 	= 0;
	  my $ingested_count 		= 0;
	  my $replicated_count 	= 0;
	  my $escrow_count 		= 0;
	  my $failed_count 		= 0;
	
	  # Get the number of tasks that we need to check
	  my $task_count = @{$astor->get_value( "astors" )};
	
	  # Process each of the sub-tasks and then check the status to see if we
	  # have finished.
	  foreach my $subtask (@{$astor->get_value( "astors" )})
	  {
		    my $childid = $subtask->get_value("astorid");
		    my $docid =  $subtask->get_value("docid");

		    # Now we've checked the sub-task get its status
		    # so we can check if the parent task has completed.
		    my $status = $subtask->get_value("astor_status");

		    if ($status eq "archive_scheduled") 
		    {
			      $scheduled_count = $scheduled_count + 1;
		    }
		    elsif ($status eq "ingest_in_progress") 
		    {
			      $inprogress_count = $inprogress_count + 1;
		    }
		    elsif ($status eq "ingested") 
		    {
			      $ingested_count = $ingested_count + 1;
		    }
		    elsif ($status eq "replicated") 
		    {
			      $ingested_count = $ingested_count + 1;
			      $replicated_count = $replicated_count + 1;
		    }
		    elsif($status eq "escrow") 
		    {
			      $replicated_count = $replicated_count + 1;
			      $escrow_count = $escrow_count + 1;
		    }
		    elsif ($status eq "archive_failed") 
		    {
			      $failed_count = $failed_count + 1;
		    }
	  }

	  # Now check to see if we need to change the 
	  # status of the eprint
	  my $astor_status = $astor->get_value("astor_status");

	  # Make sure we have sub-tasks to process before we update the main task status
	  if ($task_count > 0) {
		  if ($astor_status eq "ingest_in_progress") 
		  {
			    if ($task_count == $ingested_count) 
			    {
				      $self->_update_astor_eprint_record($astorid, "astor_status", "ingested");
			    }
		  }
		  elsif($astor_status eq "ingested") 
		  { 
			  if ($task_count == $replicated_count) 
			  {
				    $self->_update_astor_eprint_record($astorid, "astor_status", "replicated");
			  }
		  }
		  elsif($astor_status eq "replicated") 
		  {
			  if ($task_count == $escrow_count) 
			  {
			      # We have now finished replicating so we remove the local copy
			      my $ok = $self->_remove_local_eprint_copy($eprintid);

            if ($ok)
            {
		          $self->_update_astor_eprint_record($astorid, "astor_status", "escrow");
		          $self->_update_eprint_record($eprintid, "archive_status", "archived");
			        $self->_update_eprint_history($eprintid, $astorid, "archive_successful");
		        }
			  }
		  }
	  }
	
	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_eprint_restore_checker

Check the status of a restore request 

=back

=cut

#####################################################################

sub astor_eprint_restore_checker
{
	  my( $self, $eprintid, $astorid ) = @_;
	
	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }
	
	  my $inprogress_count 	= 0;
	  my $restored_count 	  = 0;
	  my $failed_count 		  = 0;
	
	  # Get the number of tasks that we need to check
	  my $task_count = @{$astor->get_value( "astors" )};
	
	  # Process each of the sub-tasks and then check the status to see if we
	  # have finished.
	  foreach my $subtask (@{$astor->get_value( "astors" )})
	  {
		    # Now we've checked the sub-task get its status
		    # so we can check if the parent task has completed.
		    my $status = $subtask->get_value("astor_status");

		    if ($status eq "restore_in_progress") 
		    {
			      $inprogress_count = $inprogress_count + 1;
		    }
		    elsif($status eq "restored") 
		    { 
			      $restored_count = $restored_count + 1;
		    }
		    elsif($status eq "restore_failed") 
		    { 
			      $failed_count = $failed_count + 1;
		    }
	  }

	  # Now check to see if we need to change the 
	  # status of the eprint
	  my $astor_status = $astor->get_value("astor_status");
	  
	  # Make sure we have sub-tasks to process before we update the main task status
	  if ($task_count > 0) 
	  {
	      # Check if all of the document tasks have finished
		    if ($astor_status eq "restore_in_progress") 
		    {
			      if ($task_count == $restored_count) 
			      {
				        $self->_update_astor_eprint_record($astorid, "astor_status", "restored");
				        $self->_update_eprint_history($eprintid, $astorid, "restore_successful");
				        $self->_send_email($eprintid, $astorid);
			      }
		    }
        
        # Check if any of the document tasks have finished
		    if ($failed_count > 0) 
		    {
		        $self->_update_astor_eprint_record($astorid, "astor_status", "restore_failed");
		    }
		    
		    # We still have document tasks that are scheduled but not yet finished 
		    # so we do nothing until the next check.
	  }
	
	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_eprint_delete_checker

Check the status of a delete request

=back

=cut

#####################################################################

sub astor_eprint_delete_checker
{
	  my( $self, $eprintid, $astorid ) = @_;
	
	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("EPrint: $eprintid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }
	
	  my $inprogress_count 	= 0;
	  my $deleted_count 	  = 0;
	  my $failed_count 		  = 0;
	
	  # Get the number of tasks that we need to check
	  my $task_count = @{$astor->get_value( "astors" )};
	
	  # Process each of the sub-tasks and then check the status to see if we
	  # have finished.
	  foreach my $subtask (@{$astor->get_value( "astors" )})
	  {
		    # Now we've checked the sub-task get its status
		    # so we can check if the parent task has completed.
		    my $status = $subtask->get_value("astor_status");

		    if ($status eq "delete_in_progress") 
		    {
			      $inprogress_count = $inprogress_count + 1;
		    }
		    elsif($status eq "deleted") 
		    { 
			      $deleted_count = $deleted_count + 1;
		    }
		    elsif($status eq "delete_failed") 
		    {
			      $failed_count = $failed_count + 1;
		    }
	  }

	  my $astor_status = $astor->get_value("astor_status");
	  
	  # Make sure we have sub-tasks to process before we update the main task status
	  if ($task_count > 0) 
	  {
		    if ($astor_status eq "delete_in_progress") 
		    {
			      if ($task_count == $deleted_count) 
			      {
				        $self->_update_astor_eprint_record($astorid, "astor_status", "deleted");
				        $self->_update_eprint_history($eprintid, $astorid, "delete_successful");
			      }
		    }

		    if ($failed_count > 0) 
		    {
		        $self->_update_astor_eprint_record($astorid, "astor_status", "delete_failed");
		    }
	  }
	
	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_doc_copy

Copy a specific ePrint document to the A-Stor service.

This event task will copy the ePrints document specified by the docid
and astorid to the A-Stor service.

If this event succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_doc_copy
{
	  my( $self, $docid, $astorid ) = @_;
	
	  # Get the repository
	  my $repository = $self->{repository};

	  # Get the document we need to copy
	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc )
	  {
		    $self->_log("Document: $docid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the storage controller object
	  my $storage = $repository->get_storage();
	  if ( not defined $storage )
	  {
		    $self->_log("astor_doc_copy: Could not get the storage controller for Document $docid...");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the specific ArkivumStorage plugin
	  my $plugin = $repository->plugin( "Storage::ArkivumStorage" );
	  if ( not defined $plugin )
	  {
		    $self->_log("astor_doc_copy: Could not get the A-Stor plugin for Document $docid...");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the status info of the A-Stor server
	  my $json = $self->_astor_getStatusInfo();
	  if ( not defined $json ) {
		    $self->_log("astor_doc_copy: A-Stor service not available..");
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
		    $self->_log("astor_doc_copy: Not enough freespace on A-Stor, copy aborted...");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Copy all files attached to the document
	  foreach my $file (@{$doc->get_value( "files" )})
	  {
		    # Get the remapped file path so we can find it within A-Stor
		    my $filename = $self->_map_to_astor_path($file->get_local_copy());

		    # Copy the file to A-Stor
		    my $ok = $storage->copy( $plugin, $file);
		    if (not $ok) 
		    {
			      $self->_log("astor_doc_copy: Error copying $filename to A-Stor...");
			      return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		    }

		    # Commit the changes to the file object otherwise it doesn't persist
		    $file->commit();
	  }
	
	  # Update the astor record to indicate where we are with the ingest
	  $self->_update_astor_record($astorid, "astor_status", "ingest_in_progress");

	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_doc_status_checker

Check the status of a copy for a specific ePrint document.

If this event succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_doc_status_checker
{
	  my( $self, $docid, $astorid ) = @_;

	  my $repository = $self->{repository};

	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc ) 
	  {
		    $self->_log("Document $docid not found in astor_doc_status_checker");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("Astor record $astorid not found in astor_doc_status_checker");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }
	
	  # Get the current state of the astor record of the document
	  my $astor_status = $astor->get_value( "astor_status" );

	  # We need to check the relevant status for each File attached to the document
	  # We will assume that that astor_status will move on only when all files
	  # attached to the documeht have reached that state.
	
	  my $state_count = 0;
	  my $file_count = @{$doc->get_value( "files" )};
	
	  # We need to store the A-Stor MD5 checksums so we can update the astor record.
	  my @values;
	
	  # We need to check all files attached to a document. We then check the A-Stor state
	  # for each file and keep a count. If the count equals the number of files attached
	  # to the document then we can move the status on.
	  foreach my $file (@{$doc->get_value( "files" )})
	  {
		    # Get the remapped file path so we can find it within A-Stor
		    my $filename = $self->_map_to_astor_path($file->get_local_copy());
		
		    # Search for the file information so we can extract the state values we need
		    my $fileInfo = $self->_astor_getFileInfo($filename);
		    if ( not defined $fileInfo )
		    {
			      $self->_log("astor_doc_status_checker: Error getting file info from A-Stor for Document $docid..");
			      return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		    }
		
		    # Check we have some results before we try to get them. We should have one result
		    my $rcount = @{$fileInfo->{"results"}};
		    if ( $rcount ne 1 )
		    {
			      $self->_log("astor_doc_status_checker: No file info returned from A-Stor for Document $docid. This file may be in the process of being removed.");
	          return EPrints::Const::HTTP_OK;
		    }
		
		    # Get the ingest and replication status values from A-Stor
		    my $ingestState = @{$fileInfo->{"results"}}[0]->{"ingestState"};
		    my $replState   = @{$fileInfo->{"results"}}[0]->{"replicationState"};
		    my $astorMD5	= @{$fileInfo->{"results"}}[0]->{"MD5checksum"};
		
	      if ($astor_status eq "ingest_in_progress") 
	      {
		        if ( $ingestState eq "FINAL" ) 
		        {
			          # We should check the MD5 Checksum of the file in both EPrints and A-Stor 
			          # to ensure they are the same. If they are not then we report this and 
			          # stop the copy process

			          # First get the md5 checksum from the eprints file
			          # We will need to check it exists and that its type is md5
			          # if it does not exist or its not md5 then we generate one
			          my $hashType = $file->get_value( "hash_type" );

			          if ( not defined $hashType or $hashType ne 'MD5' ) 
			          {
				            $file->update_md5();
				            $file->commit();
			          }

			          my $eprintsMD5 = $file->get_value( "hash" );
	
			          if ( $eprintsMD5 ne $astorMD5 ) 
			          {
				            $self->_log("astor_doc_status_checker: File checksum in eprints does not match A-Stor for $filename in document $docid..");
				            $self->_update_astor_record($astorid, "astor_status", "archive_failed");
				            return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
			          }
			
			          # The MD5 checksums match so store the A-Stor checksum so we
			          # can update the astor record once we've finished.
			          push @values, { filename => $filename, hash => $astorMD5 }; 
			
		            # Update the status counter to indicate that this file has been process successfully.
			          $state_count = $state_count + 1;
		        }
	      }
	      elsif ($astor_status eq "ingested") 
	      {
		        if ( $replState eq "amber" ) 
		        {
			          $state_count = $state_count + 1;
		        }

		        # We may have replicated already so fix the status value
		        if ( $replState eq "green" ) 
		        {
			          $astor_status = "replicated";
			          $state_count = $state_count + 1;
		        }
	      }
	      elsif($astor_status eq "replicated")
	      { 
		      if ( $replState eq "green" ) 
		      {
			        $state_count = $state_count + 1;
		      }
	      }
	  }

	  # If all files have the required state then we can change the astor_status
	  if ( $state_count == $file_count) 
	  {
        if ($astor_status eq "ingest_in_progress") 
        {
	          # Add the checksums to the astor record
	          $astor->set_value( "hash", \@values );

	          # Set the astor status
	          $astor->set_value("astor_status", "ingested");
	          $astor->commit();
        }
        elsif ($astor_status eq "ingested")
        { 
	          $self->_update_astor_record($astorid, "astor_status", "replicated");
        }
        elsif ($astor_status eq "replicated")
        { 
	          $self->_update_astor_record($astorid, "astor_status", "escrow");
	          $self->_update_document_record($docid, "archive_status", "archived");
        }
        elsif ($astor_status eq "delete_in_progress")
        {
	          $self->_update_astor_record($astorid, "astor_status", "deleted");
	          $self->_update_document_record($docid, "archive_status", "deleted");
        }
	  }

	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_doc_delete

Delete a specific ePrint document from the A-Stor service.

This event task will delete the ePrints document specified by the docid
and astorid from the A-Stor service.

If this task succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_doc_delete
{
	  my( $self, $docid, $astorid ) = @_;

	  my $repository = $self->{repository};

	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc ) 
	  {
		    $self->_log("astor_doc_delete: Document $docid not found...");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the storage controller object
	  my $storage = $repository->get_storage();
	  if ( not defined $storage )
	  {
		    $self->_log("astor_doc_delete: Could not get the storage controller for Document $docid...");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the specific ArkivumStorage plugin
	  my $plugin = $repository->plugin( "Storage::ArkivumStorage" );
	  if ( not defined $plugin )
	  {
		    $self->_log("astor_doc_delete: Could not get the A-Stor plugin for Document $docid...");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the status info of the A-Stor server
	  my $json = $self->_astor_getStatusInfo();
	  if ( not defined $json ) 
	  {
		    $self->_log("astor_doc_delete: A-Stor service not available..");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Process all files attached to the document
	  foreach my $file (@{$doc->get_value( "files" )})
	  {
		    # Delete the file stored on the A-Stor plugin
		    my $filename = $self->_map_to_astor_path($file->get_local_copy());
		
		    my $ok = $storage->delete_copy( $plugin, $file );
		    if (not $ok) 
		    {
			      $self->_log("astor_doc_delete: Error deleting $filename from A-Stor...");
			      return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		    }
		
		    # Commit the changes to the file object otherwise it doesnt't persist
		    $file->commit();
	  }

	  # Update the A-Stor record to indicate where we are with the ingest
	  $self->_update_astor_record($astorid, "astor_status", "delete_in_progress");

	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_doc_delete_checker

Check the status of a deletion from the A-Stor service for a specific 
ePrints document.

If this task succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_doc_delete_checker
{
	  my( $self, $docid, $astorid ) = @_;

	  my $repository = $self->{repository};

	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc ) 
	  {
		    $self->_log("astor_doc_delete_checker: Document $docid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("astor_doc_delete_checker: Astor record $astorid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the current state of the astor record of the document
	  my $astor_status = $astor->get_value( "astor_status" );

	  # We need to check that each file of the document object has been removed.
	  # Once they've all gone then we can move the status on.
	
	  my $state_count = 0;

	  # Process each file attached to the eprints document
	  foreach my $file (@{$doc->get_value( "files" )})
	  {
		    # Get the remapped file path so we can find it within A-Stor
		    my $filename = $self->_map_to_astor_path($file->get_local_copy());
		
		    # Search for the file information so we can extract the state values we need
		    my $fileInfo = $self->_astor_getFileInfo($filename);
		    if ( not defined $fileInfo )
		    {
			      $self->_log("astor_doc_delete_checker: Error getting file info from A-Stor for Document $docid..");
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
			      $self->_update_astor_record($astorid, "astor_status", "deleted");
			      $self->_update_document_record($docid, "archive_status", "deleted");
		    }
	  }

	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_doc_restore_request

Make a REST API Call to A-Stor to get a copy of a document restored from
the A-Stor service to the local appliance without downloading it. This is
to allow a file to be restored for later access.

If this task succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_doc_restore_request
{
	  my( $self, $docid, $astorid ) = @_;

	  my $repository = $self->{repository};

	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc ) 
	  {
		    $self->_log("astor_doc_restore_request: Document $docid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("astor_doc_restore_request: Astor record $astorid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

    # We need the path to where the files are store in A-Stor so we need to construct this
    my $main = $doc->get_stored_file($doc->get_main());
	  if ( not defined $main ) 
	  {
		    $self->_log("astor_doc_restore_request: Could not get a the main file for document $docid");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

    # called the _filename to get the correct path and file name 
    # in A-Stor
	  my( $path, $fn ) = $self->_filename( $main, $main->get_value("filename"));
	  
	  # Make the request to restore the path
	  my $requestInfo = $self->_astor_postRestoreRequest($path);
	  if ( not defined $requestInfo )
	  {
		    $self->_log("astor_doc_restore_request: Error getting request info from A-Stor for Document path $path");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }
	
	  # We have a valid response so we need to extract the request id and store it
	  # so we can check it later
	  my $requestId = $requestInfo->{'id'};
	
	  $self->_update_astor_record($astorid, "astor_request_uuid", $requestId);
		
	  $self->_update_astor_record($astorid, "astor_status", "restore_in_progress");

	  return EPrints::Const::HTTP_OK;
}


######################################################################

=over 4

=item astor_doc_restore_checker

Check the status of a restore request from the A-Stor service for a specific 
ePrint document.

If this task succeeds then it will return HTTP_OK, otherwise it will log an error 
message and return HTTP_INTERNAL_SERVER_ERROR and fail the event tazk.

=back

=cut

#####################################################################

sub astor_doc_restore_checker
{
	  my( $self, $docid, $astorid ) = @_;

	  my $repository = $self->{repository};

	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc ) 
	  {
		    $self->_log("astor_doc_restore_checker: Document $docid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("astor_doc_restore_checker: Astor record $astorid not found");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }

	  # Get the request id from the astor record
	  my $requestId = $astor->get_value("astor_request_uuid");

	  # Get the request status
	  my $requestInfo = $self->_astor_getRestoreRequest($requestId);
	  if ( not defined $requestInfo )
	  {
		    $self->_log("astor_doc_restore_request: Error getting restore request info from A-Stor for Document $docid");
		    return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
	  }
    my $status = $requestInfo->{'status'};
	
	  # If the request has not yet completed then we go no further
	  if ( $status ne "Completed" ) 
	  {
	      return EPrints::Const::HTTP_OK;
	  }
    
    # The request has completed so we now check the files of the document to see
    # if all of them are local. If they are then we have finished
	
	  my $state_count = 0;
	  my $file_count = @{$doc->get_value( "files" )};

	  # Process each file attached to the document
	  foreach my $file (@{$doc->get_value( "files" )})
	  {
		    # Get the remapped file path so we can find it within A-Stor
	      my( $path, $fn ) = $self->_filename( $file, $file->get_value("filename"));
	      
	      if ( defined $path) 
	      {
		        my $filename = $path . "/" . $fn;
		
		        # Search for the file information so we can extract the state values we need
		        my $fileInfo = $self->_astor_getFileInfo($filename);
		        if ( not defined $fileInfo )
		        {
			          $self->_log("astor_doc_restore_checker: Error getting file info from A-Stor for Document $docid..");
			          return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		        }

            # Check we have some results returned from A-Stor
		        my $rcount = @{$fileInfo->{"results"}};
		        if ( $rcount ne 1 )
		        {
			          $self->_log("astor_doc_restore_checker: No file info returned from A-Stor for Document $docid..");
			          return EPrints::Const::HTTP_INTERNAL_SERVER_ERROR;
		        }
		
		        # Get the local property of the file and test it
		        my $local = @{$fileInfo->{"results"}}[0]->{"local"};
		
		        if ( $local eq "true" )
		        {
		            $state_count = $state_count + 1;
		        }
	      }
	  }
	
	  if ( $file_count == $state_count ) 
	  {
		    $self->_update_astor_record($astorid, "astor_status", "restored");
		    $self->_update_document_record($docid, "archive_status", "restored");
	  }
    
	  return EPrints::Const::HTTP_OK;
}


sub _process_eprint_requests
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
			    my( $session, $dataset, $eprint ) = @_;

			    # Create an Event Task to process the requested action for this EPrint
			    $repository->dataset( "event_queue" )->create_dataobj({
				      pluginid => "Event::Arkivum",
				      action => $action,
				      params => [$eprint->get_value("eprintid"), $eprint->get_value("astorid")], });
		    }
	  );
   
	  $results->dispose;

	  return;
}


sub _process_doc_requests
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


sub _remove_local_eprint_copy
{
	  my( $self, $eprintid ) = @_;

    my $plugin_local = "Storage::Local";
    my $plugin_astor = "Storage::ArkivumStorage";

	  my $repository = $self->{repository};

	  # Get the eprint we need to process
	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint )
	  {
		    $self->_log("_remove_local_eprint_copy: EPrint $eprintid not found");
		    return 0;
	  }

	  # Get the storage controller object
	  my $storage = $repository->get_storage();
	  if ( not defined $storage )
	  {
		    $self->_log("_remove_local_eprint_copy: Could not get the storage controller...");
		    return 0;
	  }

	  # Get the specific ArkivumStorage plugin
	  my $plugin = $repository->plugin( $plugin_local );
	  if ( not defined $plugin )
	  {
		    $self->_log("_remove_local_eprint_copy: Could not get the Storage::Local...");
		    return 0;
	  }
	
	  foreach my $doc ($eprint->get_all_documents())
	  {
	      foreach my $file (@{$doc->value('files')})
	      {
            my $filename = $file->get_value("filename");
            my $remove_copy = 0;

            # Check we have a copy in A-Stor before we remove
            # the copy on Storage::Local
		        foreach my $copy (@{$file->value('copies')})
		        {
		            my $pluginid = $copy->{'pluginid'};

		            if ($pluginid eq $plugin_astor)
		            {
		                $remove_copy = 1;
		            }
		        }
		      
		        if ($remove_copy == 1) 
		        {
		          my $ok = $storage->delete_copy($plugin, $file);
		          if ($ok)
		          {
		              $file->commit();
		          }
		        }
	      }
	  }
	  
	  return 1;
}


sub _update_astor_eprint_record
{
	  my( $self, $astorid, $key, $value ) = @_;

	  my $repository = $self->{repository};

	  my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("Astor record $astorid not found in _update_astor_record");
		    return 0;
	  }
	
	  # We have a record so update the field value
	  $astor->set_value($key, $value);
	  return $astor->commit();
}


sub _update_astor_record
{
	  my( $self, $astorid, $key, $value ) = @_;

	  my $repository = $self->{repository};

	  my $astor = new EPrints::DataObj::Astor( $repository, $astorid );
	  if ( not defined $astor ) 
	  {
		    $self->_log("Astor record $astorid not found in _update_astor_record");
		    return 0;
	  }
	
	  # We have a record so update the field value
	  $astor->set_value($key, $value);
	  return $astor->commit();
}


sub _update_eprint_record
{
	  my( $self, $eprintid, $key, $value ) = @_;

	  my $repository = $self->{repository};

	  my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );
	  if ( not defined $eprint ) 
	  {
		    $self->_log("EPrint $eprintid not found in __update_eprint_record");
		    return 0;
	  }
	
	  # We have a record so update the field value
	  $eprint->set_value($key, $value);
	  return $eprint->commit();
}


sub _update_document_record
{
	  my( $self, $docid, $key, $value ) = @_;

	  my $repository = $self->{repository};

	  my $doc = new EPrints::DataObj::Document( $repository, $docid );
	  if ( not defined $doc ) 
	  {
		    $self->_log("Document $docid not found in __update_documenht_record");
		    return 0;
	  }
	
	  # We have a record so update the field value
	  $doc->set_value($key, $value);
	  return $doc->commit();
}


sub _update_eprint_history
{
	  my ( $self, $eprintid, $astorid, $messageid ) = @_;

	  my $repository = $self->repository;

    # Get the eprint we need to process
    my $eprint = $repository->eprint( $eprintid );
    if ( not defined $eprint )
    {
	      $self->_log("_update_eprint_history: eprint $eprintid not found");
	      return;
    }

    my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
    if ( not defined $astor ) 
    {
	      $self->_log("_update_eprint_history: astor_eprint $astorid not found");
	      return;
    }

    if ( not defined $messageid ) 
    {
	      $self->_log("_update_eprint_history: messageid $messageid not found");
	      return;
    }
  
    # Construct a history event for the restore request
	  my $history_ds = $repository->get_dataset( "history" );

	  my %hitem;

	  $hitem{message} = $self->html_phrase(
	      $messageid,
	      citation => $eprint->render_citation
	  );

	  $history_ds->create_object( 
		    $self->{session},
		    {
			      userid=>undef,
			      datasetid=>"eprint",
			      objectid=>$eprint->get_id,
			      revision=>$eprint->get_value( "rev_number" ),
			      action=>"note",
			      details=> EPrints::Utils::tree_to_utf8( $hitem{message} , 80 ),
		    }
	  );
}

 
sub _send_email 
{
	  my ( $self, $eprintid, $astorid ) = @_;

	  my $repository = $self->repository;

    # Get the eprint we need to process
    my $eprint = $repository->eprint( $eprintid );
    if ( not defined $eprint )
    {
	      $self->_log("_send_email: eprint $eprintid not found");
	      return;
    }

    my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
    if ( not defined $astor ) 
    {
	      $self->_log("_send_email: astor_eprint $astorid not found");
	      return;
    }

    my $mail_body = $self->_generate_email($eprintid, $astorid);

    my $subject = "Plugin/Event/Arkivum:restore_notification";   # id of the subject line text

    my $userid  = $astor->get_value("userid");

    my $ds = $repository->dataset( "user" );

    $ds->search(
        filters => [
            { meta_fields => [qw( userid )], value => $userid },
        ],
    )->map(sub {
        my( undef, undef, $user ) = @_;
        $user->mail( $subject, $mail_body,undef,undef );
    });

    return 1;
}

 
sub _generate_email
{
	  my ( $self, $eprintid, $astorid ) = @_;

	  my $repository = $self->repository;

    # Get the eprint we need to process
    my $eprint = $repository->eprint( $eprintid );
    if ( not defined $eprint )
    {
	      $self->_log("_generate_email: eprint $eprintid not found");
	      return;
    }

    my $astor = new EPrints::DataObj::AstorEPrint( $repository, $astorid );
    if ( not defined $astor ) 
    {
	      $self->_log("_generate_email: astor_eprint $astorid not found");
	      return;
    }

	  my $xml = $repository->xml;
 
	  my $content = $xml->create_document_fragment();
	  my $h1 = $xml->create_element("h1");

    $content->appendChild($h1);

    $h1->appendChild($repository->make_text("EPrint Restore Notification"));
 
    my $ds = $repository->dataset( "eprint" );
 
    my $date = time() - 86400; # 1 day ago
    $date = EPrints::Time::iso_date( $date ) . " 00:00:00";
 
    my $eprint_section = $xml->create_document_fragment();
 
    $eprint_section->appendChild($eprint->render_value("title"));
    $eprint_section->appendChild($eprint->render_citation_link());
    $eprint_section->appendChild($repository->make_text(" ("));
    $eprint_section->appendChild($eprint->render_value("eprint_status"));
    $eprint_section->appendChild($repository->make_text(")"));
    $eprint_section->appendChild($xml->create_element("br"));
    $eprint_section->appendChild($xml->create_element("br"));
    $eprint_section->appendChild($repository->make_text("Has been restored and is now available."));

    $content->appendChild($eprint_section);
 
    return $content;       
}


sub _astor_getStatusInfo 
{
	  my( $self ) = @_;

	  my $api_url = "/json/status/info/";
	
	  my $response = $self->_astor_getRequest($api_url);
	  if ( not defined $response )
	  {
		    $self->_log("Inavlid response returned in __astor_getStatus");
		    return;
	  }

	  if ($response->is_error) 
	  {
		    $self->_log("Invalid response returned in __astor_getStatus: $response->status_line");
		    return;
	  }
    
	  # Get the content which should be a json string
	  my $json = decode_json($response->content);
	  if ( not defined $json) {
		    $self->_log("Invalid response returned in __astor_getStatus");
		    return;
	  }
    
	  return $json;
}


sub _astor_getFileInfo
{
	  my( $self, $filename) = @_;

	  my $api_url = "/json/search/files?path=" . $filename;

	  my $response = $self->_astor_getRequest($api_url);
	  if ( not defined $response )
	  {
		    $self->_log("_astor_getFileInfo: Invalid response returned...");
		    return;
	  }

	  if ($response->is_error) 
	  {
		    $self->_log("_astor_getFileInfo: Invalid response returned: $response->status_line");
		    return;
	  }

	  # Get the content which should be a json string
	  my $json = decode_json($response->content);
	  if ( not defined $json) 
	  {
		    $self->_log("_astor_getFileInfo: Invalid response returned...");
		    return;
	  }
    
	  return $json;
}


sub _astor_getFilePathInfo
{
	  my( $self, $path) = @_;

	  if ( not defined $path )
	  {
		    $self->_log("_astor_getFilePathInfo: No path specified");
		    return;
	  }

	  my $api_url = "/files/" . $path;
	  
	  my $response = $self->_astor_getRequest($api_url);
	  if ( not defined $response )
	  {
		    $self->_log("_astor_getFilePathInfo: Invalid response returned...");
		    return;
	  }

	  if ($response->is_error) 
	  {
		    $self->_log("_astor_getFilePathInfo: Invalid response returned: $response->status_line");
		    return;
	  }

	  # Get the content which should be a json string
	  my $json = decode_json($response->content);
	  if ( not defined $json) 
	  {
		    $self->_log("_astor_getFilePathInfo: Invalid response returned...");
		    return;
	  }
    
	  return $json;
}


sub _astor_postRestoreRequest
{
	  my( $self, $path) = @_;

	  if ( not defined $path )
	  {
		    $self->_log("_astor_postRestoreRequest: No path specified");
		    return;
	  }

    # First we need to get the UUID of the folder in A-Stor
    # so we can make the restore request
	  my $response = $self->_astor_getFilePathInfo($path);
	  if ( not defined $response )
	  {
		    $self->_log("_astor_postRestoreRequest: Invalid response returned...");
		    return;
	  }

    # We have a response in json so we can now get the UUID we need
	  my $UUID = @{$response->{"files"}}[0]->{"id"};

	  my $api_url = "/api/2/local-cache/restore-request/" . $UUID;

	  $response = $self->_astor_postRequest($api_url);
	  if ( not defined $response )
	  {
		    $self->_log("_astor_postRestoreRequest: Invalid response returned...");
		    return;
	  }

	  if ($response->is_error) 
	  {
		    $self->_log("_astor_postRestoreRequest: Invalid response returned: $response->status_line");
		    return;
	  }

	  # Get the content which should be a json string
	  my $json = decode_json($response->content);

	  if ( not defined $json) 
	  {
		    $self->_log("_astor_postRestoreRequest: Invalid response returned...");
		    return;
	  }
    
	  return $json;
}


sub _astor_getRestoreRequest
{
	  my( $self, $requestId) = @_;

	  if ( not defined $requestId )
	  {
		    $self->_log("_astor_getRestoreRequest: No request id specified");
		    return;
	  }

	  my $api_url = "/api/2/local-cache/restore-request/" . $requestId;

	  my $response = $self->_astor_getRequest($api_url);
	  if ( not defined $response )
	  {
		    $self->_log("_astor_getRestoreRequest: Invalid response returned...");
		    return;
	  }

	  if ($response->is_error) 
	  {
		    $self->_log("_astor_getRestoreRequest: Invalid response returned: $response->status_line");
		    return;
	  }

	  # Get the content which should be a json string
	  my $json = decode_json($response->content);
	  if ( not defined $json) 
	  {
		    $self->_log("_astor_getRestoreRequest: Invalid response returned...");
		    return;
	  }
    
	  return $json;
}


sub _astor_getRequest 
{
	  my( $self, $url ) = @_;

	  my $ark_server = $self->param( "server_url" );
	  my $server_url = $ark_server . $url;

	  my $ua       = LWP::UserAgent->new();
	  my $response = $ua->get( $server_url );
    
	  return $response;
}


sub _astor_postRequest 
{
	  my( $self, $url ) = @_;

	  my $ark_server = $self->param( "server_url" );
	  my $server_url = $ark_server . $url;

	  my $ua       = LWP::UserAgent->new();
	  my $response = $ua->post( $server_url );
    
	  return $response;
}


sub _log 
{
	  my ( $self, $msg) = @_;

	  $self->{repository}->log($msg);
}


sub _set_debug 
{
	  my ( $self, $enabled) = @_;
	
	  my $repo = $self->{repository};
	  if ( $enabled ) 
	  {
		    $repo->{noise} = 1;
	  } else 
	  {
		    $repo->{noise} = 0;
	  }
}


sub _map_to_astor_path 
{
	  my( $self, $local_path) = @_;
	
	  # Get the root path for repository as it would be on local storage
	  my $repo = $self->{session}->get_repository;
	  my $local_root_path = $repo->get_conf( "archiveroot" );

	  # Start to build the astor path relative to the repository
	  my $astor_mount_path = '/' . $repo->id;
	
	  # Replace the local root path with the A-Stor relative path
	  my $mapped_path = $local_path;
	  $mapped_path =~ s#$local_root_path#$astor_mount_path#;
	
	  return $mapped_path;
}


sub _filename
{
	my( $self, $fileobj, $filename ) = @_;

	my $parent = $fileobj->get_parent();
	
	my $local_path;

	if( !defined $filename )
	{
		$filename = $fileobj->get_value( "filename" );
		$filename = $self->_escape_filename( $filename );
	}

	my $in_file;

	if( $parent->isa( "EPrints::DataObj::Document" ) )
	{
		$local_path = $parent->local_path;
		$in_file = $filename;
	}
	elsif( $parent->isa( "EPrints::DataObj::History" ) )
	{
		my $eprint = $parent->get_parent;
		return if !defined $eprint;
		$local_path = $eprint->local_path."/revisions";
		$filename = $parent->get_value( "revision" ) . ".xml";
		$in_file = $filename;
	}
	elsif( $parent->isa( "EPrints::DataObj::EPrint" ) )
	{
		$local_path = $parent->local_path;
		$in_file = $filename;
	}
	else
	{
		# Unknown file type
		$self->_log("Unknown file type: $parent");
	}

	# Map the path and filename to the correct path within Arkivum storage
	my $ark_path = $self->_map_to_astor_path($local_path);

	return( $ark_path, $in_file );
}


1;
