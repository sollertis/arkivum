package EPrints::Plugin::Event::Arkivum;

@ISA = qw( EPrints::Plugin::Event );

use JSON qw(decode_json);
use LWP::UserAgent;

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

   	my $repository = $self->{repository};
	
	# Process the copy approved requests
	$self->process_requests("astor_copy", "archive_approved");
	
	# Process the delete approved requests
	$self->process_requests("astor_delete", "delete_approved");
 
   	return;
}

sub process_requests
{
	my( $self, $action, $status ) = @_;

	my $repository = $self->{repository};
	
	my $ds = $repository->dataset( "document" );

	# Create search expression
	my $search = new EPrints::Search( session=>$repository, dataset=>$ds );

	# Add filter
	$search->add_field( $ds->get_field( "archive_status" ), $status, "EQ", "ALL" );
 
	# Perform the search
	my $results = $search->perform_search;

	# Get the number of search results 
	my $count = $results->count;
 
	# Get all matching ids
	my $ids = $results->get_ids; 
 
	$results->map( 
		sub {
			my( $session, $dataset, $doc ) = @_;

			# Create an Event Task to process the copy action for this EPrint
			$repository->dataset( "event_queue" )->create_dataobj({
				pluginid => "Event::Arkivum",
				action => $action,
				params => [$doc->get_value("docid")], });
		}
	);
 
	$results->dispose;
	
	return;
}


sub astor_copy
{
	my( $self, $docid ) = @_;

	my $repository = $self->{repository};

	my $doc = new EPrints::DataObj::Document( $repository, $docid );
	
	if ( defined $doc ) {
		print "Document: $docid\n";
		$doc->set_value("archive_status", "replicated");
		$doc->commit();
	}
	
#	my $json = $self->astor_getStatusInfo();
#	if ( defined $json ) {
#		print "json string returned\n";
#	}

	return;
}


sub astor_delete
{
	my( $self, $fileid ) = @_;

	my $repository = $self->{repository};

	my $file = new EPrints::DataObj::File( $repository, $fileid );

	if ( defined $file ) {
		$file->set_value("archive_status", "deleted");
		$file->commit();
	}

	return;
}


#
#	A-Stor REST API Functions
#

my $server_url  = "https://172.18.2.240:8443";
my $api_url = "/json/status/info/";
my $rest_url = $server_url . $api_url;

sub astor_getStatusInfo 
{

  my $response = $self->getRequest($rest_url);

  if ($response->is_error) {
    print "\tFailed. Error: " . $response->status_line . "\n";
	return;
  }
  
  # Get the content which should be a json string
  my $json = decode_json($response->content);

  my $os = $json->{'server'}{'os'};

  if ($os ne 'Linux') {
    print "\tFailed. Error: Server property is invalid.\n";
    return;
  }
  
  return $json;
}


sub astor_getRequest 
{
	my( $self, $url, $params ) = @_;

	my $ua       = LWP::UserAgent->new();
	my $response = $ua->get( $url, $params );
  
	return $response;
}


sub astor_postRequest 
{
	my( $self, $url, $params ) = @_;

	my $ua       = LWP::UserAgent->new();
	my $response = $ua->post( $url, $params );
  
	return $response;
}


sub astor_deleteRequest 
{
	my( $self, $url, $params) = @_;

	my $ua       = LWP::UserAgent->new();
	my $req = HTTP::Request->new(DELETE => $url);
	my $response = $ua->request($req);
  
	return $response;
}

1;