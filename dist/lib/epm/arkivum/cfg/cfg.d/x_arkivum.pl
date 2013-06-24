# To Configure your Arkivum Storage, please fill in the configuration fields below:
#
# Once done, please remove the comments '#' from the beginning of each line, save this file and then reload the repository config from the Admin screen.
# 

$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{mount_path} = "/mnt/arkivum";
$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{server_url} = "https://localhost:8443";
$c->{plugins}->{"Event::Arkivum"}->{params}->{server_url} = "https://localhost:8443";

$c->{plugins}{"Event::Arkivum"}{params}{disable} = 0;
$c->{plugins}{"Storage::ArkivumStorage"}{params}{disable} = 0;
$c->{plugins}{"Screen::EPrint::Document::AStor"}{params}{disable} = 0;
$c->{plugins}{"Screen::Workflow::AStorApprove"}{params}{disable} = 0;

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
		) ],
	}, 
);

push @{$c->{user_roles}{admin}}, qw(
	+astor/destroy
	+astor/details
	+astor/edit
	+astor/view
);

# Define the class, this can either be done using a new file in the right place, or by using this override trick, open a '{' and then continue as it this is new file
{
        package EPrints::DataObj::Astor;

        our @ISA = qw( EPrints::DataObj::SubObject );

        # The new method can simply return the constructor of the super class (Dataset)
        sub new
        {
                return shift->SUPER::new( @_ );
        }

        # This method is required to just return the dataset_id.
        sub get_dataset_id
        {
                my ($self) = @_;
                return "astor";
        }

		sub parent
		{
			my ($self) = @_;

			my $docid = $self->value('docid');
			return if !$docid;

			return $self->{session}->dataset('document')->dataobj($docid);
		}

		sub remove
		{
			my ($self) = @_;

			my $doc = $self->parent;
			if (defined $doc)
			{
				$doc->set_value('archive_status', undef);
				$doc->commit;
			}

			return $self->SUPER::remove();
		}
}

