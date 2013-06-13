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
 
       $self->SUPER::action_enable( $skip_reload );
 
       my $repo = $self->{repository};
 
       # ADD STUFF HERE
       EPrints::DataObj::EventQueue->create_unique( $repo, {
               pluginid => "Event",
               action => "cron",
               params => ["0,15,30,45,60 * * * *",
                       "Event::Arkivum",
                       "astor_checker",
               ],
       });
 
       $self->reload_config if !$skip_reload;
}
 
 
sub action_disable
{
       my( $self, $skip_reload ) = @_;
 
       $self->SUPER::action_disable( $skip_reload );
       my $repo = $self->{repository};
 
       # ADD STUFF HERE
	my $event = EPrints::DataObj::EventQueue->new_from_hash( $repo, {
               pluginid => "Event",
               action => "cron",
               params => ["0,15,30,45,60 * * * *",
                       "Event::Arkivum",
                       "astor_checker",
               ],
       });
       $event->delete if (defined $event);       
       $self->reload_config if !$skip_reload;
 
}
 
1;