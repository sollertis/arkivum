# To Configure your Arkivum Storage, please fill in the configuration fields below:
#
# Once done, please remove the comments '#' from the beginning of each line, save this file and then reload the repository config from the Admin screen.
# 
$c->{plugins}{"Event::Arkivum"}{params}{disable} = 0;
$c->{plugins}{"Storage::ArkivumR"}{params}{disable} = 0;

$c->{plugins}->{"Storage::ArkivumR"}->{params}->{mount_path} = "/mnt/archive";

$c->add_dataset_field( "document", {
		name => "archive_status",
		type => 'set',
		options => [qw(
			archive_requested
			archive_approved
			ingested
			replicated
			escrow
			archive_failed
			delete_requested
			delete_approved
			delete_in_progress
			deleted
			delete_failed
		)],
	}, 
);



