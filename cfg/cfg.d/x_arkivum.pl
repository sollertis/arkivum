# To Configure your Arkivum Storage, please fill in the configuration fields below:
#
# Once done, please remove the comments '#' from the beginning of each line, save this file and then reload the repository config from the Admin screen.
# 

#
# Enable / Disable plugins
#

$c->{plugins}{"Event::Arkivum"}{params}{disable} = 0;
$c->{plugins}{"Storage::ArkivumStorage"}{params}{disable} = 0;

$c->{plugins}{"Screen::EPrint::AStorRestore"}{params}{disable} = 0;
$c->{plugins}{"Screen::EPrint::AStorDelete"}{params}{disable} = 0;
$c->{plugins}{"Screen::Workflow::AStorEPrintApprove"}{params}{disable} = 0;

$c->{plugins}{"Screen::EPrint::Document::AStor"}{params}{disable} = 1;
$c->{plugins}{"Screen::Workflow::AStorApprove"}{params}{disable} = 0;


# 
# Where the A-Stor appliance is mounted on the local file system E.g. /mnt/arkivum
# This folder should be accessed for read / write by Apache, ePrints, and the Indexer
# 

$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{mount_path} = "";

#
# The URL of the A-Stor appliance. E.g. https://172.18.2.9:8443
#

$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{server_url} = "";
$c->{plugins}->{"Event::Arkivum"}->{params}->{server_url}          = "";

# The redirect URL used to confirm a restore of a file when downloading a file from A-Stor that is not on the local appliance
$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{redirect_url} = "/cgi/users/home?screen=EPrint%3A%3AAStorRestore&eprintid=";

# The size threshold (in bytes, default is 1GB = 1024 x 1024 x 1024) used to determine if we 
# server a file directly from the data center if it is not stored on the local appliance
# If the file is less than or equal to this threshold it will be served from the data center
# directly otherwise the user is redirected to an eprint screen to request a restore.

$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{redirect_threshold} = 1073741824;


# These two parameters are used to estimate the time to restore an eprint
# to the local A-Stor cache. Factor_a is multiplied by the total size of 
# te ePrint (in bytes) which is then added to factor_b.
# 
# Both parameters are expected to be numbers in seconds

# Time to restore 1MB (1024 x 1024) in seconds
$c->{plugins}->{"Screen::EPrint::AStorRestore"}->{params}->{factor_a} = 2;

# Specify an expect wait time in seconds
$c->{plugins}->{"Screen::EPrint::AStorRestore"}->{params}->{factor_b} = 900;


# Add an archive_status to the eprint table so we can keep track
# of A-Stor archive requests for an eprint
$c->add_dataset_field( "eprint", {
		name => "archive_status",
		type => 'set',
		options => [ qw(
			archive_requested
			archive_approved
			archive_in_progress
			archived
			archive_failed
		) ],
	}, 
);

$c->add_dataset_field( "eprint", { name=>"astors", type=>"subobject", datasetid=>'astor_eprint', multiple=>1, text_index=>1, dataset_fieldname=>'', dataobj_fieldname=>'eprintid' }, );

$c->{datasets}->{astor_eprint} = {
        class => "EPrints::DataObj::AstorEPrint",
        sqlname => "astor_eprint",
        sql_counter => "astorid",
};

# Add fields to the dataset
$c->add_dataset_field( "astor_eprint", { name=>"astorid", type=>"counter", required=>1, can_clone=>0, sql_counter=>"astorid" }, );
$c->add_dataset_field( "astor_eprint", { name=>"userid", type=>"itemref", datasetid=>'user', required=>1, }, );
$c->add_dataset_field( "astor_eprint", { name=>"eprintid", type=>"itemref", datasetid=>'eprint', required=>1, }, );
$c->add_dataset_field( "astor_eprint", { name=>"justification", type=>"longtext", required=>0, }, );
$c->add_dataset_field( "astor_eprint", { name=>"access_date", type=>"time", required=>0, }, );
$c->add_dataset_field( "astor_eprint", {
		name => "astor_status",
		type => 'set',
		options => [ qw(
			archive_requested
			archive_scheduled
			ingest_in_progress
			ingested
			replicated
			escrow
			archive_failed
			delete_scheduled
			delete_in_progress
			deleted
			delete_failed
			restore_scheduled
			restore_in_progress
			restored
			restore_failed
		) ],
	}, 
);

$c->add_dataset_field( "astor_eprint", { name=>"astors", type=>"subobject", datasetid=>'astor', multiple=>1, text_index=>1, dataset_fieldname=>'', dataobj_fieldname=>'parentid' }, );


# Add an archive_status to the document object so we can keep track of 
# A-Stor archive requests for a document
#
$c->add_dataset_field( "document", {
		name => "archive_status",
		type => 'set',
		options => [ qw(
			archive_requested
			archive_approved
			archived
			archive_failed
			delete_requested
			delete_approved
			deleted
			delete_failed
			restore_requested
			restore_approved
			restored
			restore_failed
		) ],
	}, 
);

$c->add_dataset_field( "document", { name=>"astors", type=>"subobject", datasetid=>'astor', multiple=>1, text_index=>1, dataset_fieldname=>'', dataobj_fieldname=>'docid' }, );

$c->{datasets}->{astor} = {
        class => "EPrints::DataObj::Astor",
        sqlname => "astor",
        sql_counter => "astorid",
};

# Add fields to the dataset
$c->add_dataset_field( "astor", { name=>"astorid", type=>"counter", required=>1, can_clone=>0, sql_counter=>"astorid" }, );
$c->add_dataset_field( "astor", { name=>"userid", type=>"itemref", datasetid=>'user', required=>1, }, );
$c->add_dataset_field( "astor", { name=>"docid", type=>"itemref", datasetid=>'document', required=>1, }, );
$c->add_dataset_field( "astor", { name=>"parentid", type=>"itemref", datasetid=>'astor_eprint', required=>0, }, );
$c->add_dataset_field( "astor", {
	name => "hash",
	type => "multipart",
	fields => [
		{ sub_name => "filename", type => "id", },
		{ sub_name => "hash", type => "id", },
	],
	multiple => 1,
});
$c->add_dataset_field( "astor", { name=>"justification", type=>"longtext", required=>0, }, );
$c->add_dataset_field( "astor", { name=>"access_date", type=>"time", required=>0, }, );
$c->add_dataset_field( "astor", { name=>"astor_request_uuid", type=>"text", required=>0, }, );
$c->add_dataset_field( "astor", {
		name => "astor_status",
		type => 'set',
		options => [ qw(
			archive_scheduled
			ingest_in_progress
			ingested
			replicated
			escrow
			archive_failed
			delete_scheduled
			delete_in_progress
			deleted
			delete_failed
			restore_scheduled
			restore_in_progress
			restored
			restore_failed
		) ],
	}, 
);


$c->add_dataset_trigger( "eprint", EP_TRIGGER_STATUS_CHANGE , 
    sub 
    {
        my ( %params ) = @_;
        my $repository = $params{repository};

        return undef if (!defined $repository);

		    if (defined $params{dataobj})
		    {
			      my $dataobj = $params{dataobj};
			      my $eprintid = $dataobj->id;
			
			      # Get the eprint object so we can check the status
			      my $eprint = new EPrints::DataObj::EPrint( $repository, $eprintid );

			      # The status has changed and its now archived so we create a 
			      # request to store the eprint in A-Stor
			      if ( defined $eprint ) 
			      {
				        my $status = $eprint->get_value( "eprint_status" );
				        if ( $status eq 'archive' ) 
				        {
					          $repository->dataset( "astor_eprint" )->create_dataobj(
					          {
						            eprintid => $eprintid,
						            userid => $repository->current_user->id,
						            justification => 'EPrint A-Stor Archive Request',
						            astor_status => 'archive_scheduled',
					          });
					          
					          # Update the eprint archive_status field so we know
					          # that the A-Stor request has been made
					          $eprint->set_value("archive_status", "archive_approved");
					          $eprint->commit();
				        }
			      }
		    }
    }
);

 

push @{$c->{user_roles}{admin}}, qw(
	+astor_eprint/destroy
	+astor_eprint/details
	+astor_eprint/edit
	+astor_eprint/view
	+astor/destroy
	+astor/details
	+astor/edit
	+astor/view
);
