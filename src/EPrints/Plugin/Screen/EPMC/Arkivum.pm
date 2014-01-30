package EPrints::Plugin::Screen::EPMC::Arkivum;
 
@ISA = ( 'EPrints::Plugin::Screen::EPMC' );
 
use strict;

sub new
{
    my( $class, %params ) = @_;
 
    my $self = $class->SUPER::new( %params );
 
    $self->{actions} = [qw( enable disable )];
    $self->{disable} = 0; # always enabled, even in lib/plugins

    $self->{package_name} = "Arkivum";

    return $self;
}
 
  
sub action_enable
{
    my( $self, $skip_reload ) = @_;
    my $repo = $self->{repository};

    #before enabling, make sure we have all dependant libs installed
  	my @prereqs = qw/
      Data::Dumper
      DateTime::Format::ISO8601
      File::Basename
      IO::Socket::SSL
      JSON
      LWP::UserAgent
    /;

	  my $evalstring;

	  foreach my $l (@prereqs)
	  {
		  $evalstring .= "use $l;\n";
	  }

	  eval $evalstring;
	  if (!$@)
	  {
        $self->SUPER::action_enable( $skip_reload );
        
        # Now the plugins have been enabled we need to check if they have
        # been configured so we check the parameters. If the minimal params
        # are not set then we will disable and report the issue back to the user

        EPrints::DataObj::EventQueue->create_unique( $repo, {
               pluginid => "Event",
               action => "cron",
               params => ["0,15,30,45,60 * * * *", "Event::Arkivum", "astor_checker", ],
        });
	  }
	  else
	  {
		    my $xml = $repo->xml;
		    my $msg = $xml->create_document_fragment;

		    $msg->appendChild($xml->create_text_node('Arkivum cannot be enabled because one or more of the following perl libraries are missing:'));
		    my $ul = $xml->create_element('ul');
		    $msg->appendChild($ul);

		    foreach my $l (@prereqs)
		    {
			    my $li = $xml->create_element('li');
			    $ul->appendChild($li);
			    $li->appendChild($xml->create_text_node($l));
		    }
		
		    $msg->appendChild($xml->create_text_node('Speak to your systems administrator, who may be able to install them for you.'));

		    $self->{processor}->add_message('warning',$msg);
	  }

    $self->reload_config if !$skip_reload;
}
 
 
sub action_disable
{
    my( $self, $skip_reload ) = @_;

    $self->SUPER::action_disable( $skip_reload );
    my $repo = $self->{repository};

    my $event = EPrints::DataObj::EventQueue->new_from_hash( $repo, {
           pluginid => "Event",
           action => "cron",
           params => ["0,15,30,45,60 * * * *", "Event::Arkivum", "astor_checker", ],
    });
    $event->delete if (defined $event);       
    $self->reload_config if !$skip_reload;
 
}
 
1;
