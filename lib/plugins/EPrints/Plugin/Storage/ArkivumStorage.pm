=head1 NAME

EPrints::Plugin::Storage::ArkivumStorage - storage to Arkivum assured archive service

=head1 SYNOPSIS

	# cfg.d/x_arkivum.pl
	$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{mount_path} = "...";
	$c->{plugins}->{"Storage::ArkivumStorage"}->{params}->{server_url} = "https:...";


=head1 DESCRIPTION

See L<EPrints::Plugin::Storage> for available methods.

To enable this module you must specify the mount path where the Arkivum appliance is mounted on the local file system and the appliance server url.

=head1 METHODS

=over 4

=cut

package EPrints::Plugin::Storage::ArkivumStorage;

use Fcntl qw( SEEK_SET :DEFAULT );

use EPrints::Plugin::Storage;
use JSON qw(decode_json);
use LWP::UserAgent;
use File::Path;


@ISA = ("EPrints::Plugin::Storage");

use strict;

sub new {
	my($class, %params) = @_;

	my $self = $class->SUPER::new( %params );

	$self->{name} = "Arkivum A-Stor storage";
	
	# See lib/lang/en/phrases/system.xml for storage classes
	$self->{storage_class} = "m_local_archival_storage";
	
	# Enable debug logging
	$self->_set_debug(1);
	
	return $self;
}

sub close_read
{
	my( $self, $fileobj, $sourceid, $f ) = @_;

	my $fh = delete $self->{_fh}->{$fileobj};
	close($fh);
}

sub close_write
{
	my( $self, $fileobj ) = @_;

	delete $self->{_path}->{$fileobj};

	my $fh = delete $self->{_fh}->{$fileobj};
	close($fh);

	return delete $self->{_name}->{$fileobj};
}

sub delete
{
	my( $self, $fileobj, $sourceid ) = @_;

	my( $path, $fn ) = $self->_filename( $fileobj, $sourceid );

	return undef if !defined $path;

	return 1 if !-e "$path/$fn";
	
	# Get the status info from A-Stor to check the service is available
	my $json = $self->_astor_getStatusInfo();
	if ( not defined $json ) {
		$self->_log("ArkivumStorage: A-Stor service not available..");
		return 0;
	}

	# We need to remove the file from A-Stor using the REST API
	my $filename = $self->_map_to_astor_path($fileobj->get_local_copy());

	my $response = $self->_astor_deleteFile($filename);
	if ($response->is_error) 
	{
		$self->_log("ArkivumStorage: Error invalid response returned: " . $response->status_line);
		return 0;
	}

	# remove empty leaf directories (e.g. document dir)
	my @parts = split /\//, $fn;
	pop @parts;

	for(reverse 0..$#parts)
	{
		my $dir = join '/', $path, @parts[0..$_];
		last if !rmdir($dir);
	}
	rmdir($path);

	return 1;
}

sub get_local_copy
{
	my( $self, $fileobj, $sourceid ) = @_;

	my( $path, $fn ) = $self->_local_filename( $fileobj, $sourceid );

	return undef if !defined $path;

	return -r "$path/$fn" ? "$path/$fn" : undef;
}

sub get_remote_copy
{
	my( $self, $fileobj, $sourceid ) = @_;

	my $url = $self->_get_url( $fileobj, $sourceid );

	return undef if !defined $url;

	return $url;
}

sub open_read
{
	my( $self, $fileobj, $sourceid, $f ) = @_;

	my( $path, $fn ) = $self->_filename( $fileobj, $sourceid );

	return undef if !defined $path;

	my $in_fh;
	if( !open($in_fh, "<", "$path/$fn") )
	{
		$self->{error} = "Unable to read from $path/$fn: $!";
		$self->{session}->get_repository->log( $self->{error} );
		return undef;
	}
	binmode($in_fh);

	$self->{_fh}->{$fileobj} = $in_fh;

	return 1;
}

sub open_write
{
	my( $self, $fileobj, $offset ) = @_;

	my( $path, $fn ) = $self->_filename( $fileobj );

	return undef if !defined $path;

	# filename may contain directory components
	my $local_path = "$path/$fn";
	$local_path =~ s/[^\\\/]+$//;

# This method does not work on CentOS 5.8
#	EPrints::Platform::mkdir( $local_path );

  eval { mkpath($local_path) };
  if ($@) {
		$self->{error} = "ArkivumStorage.open_write:Unable to create folder path $local_path";
		$self->{session}->get_repository->log( $self->{error} );
		return 0;
  }

	my $mode = O_WRONLY|O_CREAT;

	my $fh;
	unless( sysopen($fh, "$path/$fn", $mode) )
	{
		$self->{error} = "ArkivumStorage.open_write:Unable to write to $path/$fn: $!";
		$self->{session}->get_repository->log( $self->{error} );
		return 0;
	}

	sysseek($fh, $offset, 0) if defined $offset;

	$self->{_fh}->{$fileobj} = $fh;
	$self->{_path}->{$fileobj} = $path;
	$self->{_name}->{$fileobj} = $fn;

	return 1;
}

sub retrieve
{
	my( $self, $fileobj, $sourceid, $offset, $n, $f ) = @_;

	return 0 if !$self->open_read( $fileobj, $sourceid, $f );
	my( $path, $fn ) = $self->_filename( $fileobj, $sourceid );

	return undef if !defined $path;

	my $fh = $self->{_fh}->{$fileobj};

	my $rc = 1;

	sysseek($fh, $offset, SEEK_SET);

	my $buffer;
	my $bsize = $n > 65536 ? 65536 : $n;
	while(sysread($fh,$buffer,$bsize))
	{
		$rc &&= &$f($buffer);
		last unless $rc;
		$n -= $bsize;
		$bsize = $n if $bsize > $n;
	}

	$self->close_read( $fileobj, $sourceid, $f );

	return $rc;
}


sub write
{
	my( $self, $fileobj, $buffer ) = @_;

	use bytes;

	my $fh = $self->{_fh}->{$fileobj}
		or Carp::croak "Must call open_write before write";
	
	my $rc = syswrite($fh, $buffer);
	if( !defined $rc || $rc != length($buffer) )
	{
		my $path = $self->{_path}->{$fileobj};
		my $fn = $self->{_name}->{$fileobj};
		unlink("$path/$fn");
		$self->{error} = "Error writing to $path/$fn: $!";
		$self->{session}->get_repository->log( $self->{error} );
		return 0;
	}

	return 1;
}


sub _escape_filename
{
	my( $self, $filename ) = @_;

	# $filename is UTF-8
	$filename =~ s# /\.+ #/_#xg; # don't allow hiddens
	$filename =~ s# //+ #/#xg;
	$filename =~ s# ([:;'"\\=]) #sprintf("=%04x", ord($1))#xeg;

	return $filename;
}

sub _local_filename
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
	
	return( $local_path, $in_file );
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
	my $ark_path = $self->_map_to_ark_path($local_path);

	return( $ark_path, $in_file );
}


sub _get_url
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
	
	# We need to query the file in A-Stor to check if its on the local
	# appliance cache or stored in the data centers. If it stored only
	# on the data centers we need to redirect so that the user can
	# request a restore the appliance

  my $astorpath = $self->_map_to_astor_path($local_path) . "/" . $in_file;

  # Search for the file information so we can extract the state values we need
  my ( $local, $filesize ) = $self->_isAStorFileLocal($astorpath);

  my $ark_url;

  if ( defined $local ) 
  {
      if (!$local) 
      {
          # If the file is not local to the A-Stor appliance then
          # we redirect to an eprint page so that a restore
          # can be requested.
        	my $redirect_url = $self->param( "redirect_url" );

        	my $size_threshold = $self->param( "redirect_threshold" );
        	if (not defined $size_threshold ) {
          	  $size_threshold = 1073741824;     # default to 1GB
        	}
        	
        	# Check the size threshold of the requested file
        	# if we are less than or equal then we return undef
        	# so eprint will read the file from the appliance and
        	# stream it directly from the data center
        	if ( $filesize > $size_threshold ) 
        	{
            	# Get the eprintid so we can append to the redirect url
            	# if we can't get the eprint parent then simply pass
            	# back the url generated by A-Stor
            	print STDERR "filesize: $filesize\nsize_threshold:$size_threshold\n";
            	
	            my $gparent = $parent->get_parent();

	            if( $gparent->isa( "EPrints::DataObj::EPrint" ) )
	            {
	                # Complete the redirect url by including the eprint
	                # we will need to request the restore for
		              my $eprint = $gparent->get_value("eprintid");
		              $ark_url = $redirect_url . $eprint;
	            }
	            else
	            {
	                # We don't have a file from an EPrint so just return
	                # the URL from A-Stor
	                $ark_url = $self->_get_astor_url($local_path) . "/" . $in_file;
	            }
        	}
      }
      else 
      {
	        # We have a file on the local A-Stor appliance so
	        # we return undef as this will mean the file is
	        # read from the locally mounted file system
      }
  }
  
	return $ark_url;
}


sub _log {
	my ( $self, $msg) = @_;
	$self->{session}->get_repository->log($msg);
	print STDERR $msg . "\n";
}


sub _map_to_ark_path {
	
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


sub _map_to_astor_path {
	
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


sub _get_astor_url {
	
	my( $self, $local_path) = @_;
	
	# Get the root path for repository as it would be on local storage
	my $repo = $self->{session}->get_repository;
	my $local_root_path = $repo->get_conf( "archiveroot" );

	# Start to build the astor path relative to the repository
	my $ark_mount_path = $self->param( "server_url" );
	my $astor_mount_path = '/files/' . $repo->id;
	
	# Replace the local root path with the A-Stor relative path
	my $mapped_path = $local_path;
	$mapped_path =~ s#$local_root_path#$astor_mount_path#;
	
	return $ark_mount_path . $mapped_path;
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


sub _isAStorFileLocal
{
	  my( $self, $filepath) = @_;

	  my $ok = 0;
    
    my $fileInfo = $self->_astor_getFileInfo($filepath);

    if ( not defined $fileInfo )
    {
	      $self->_log("isAStorFileLocal: File Info not found in A-Stor for file $filepath.");
	      return;
    }
    
    my $local = $fileInfo->{"local"};
    
    if ( $local eq "true" )
    {
        $ok = 1;
    }
    
    my $filesize = $fileInfo->{"size"};

    return ($ok, $filesize);
}


sub _astor_getFileInfo
{
	  my( $self, $filename) = @_;

	  my $api_url = "/api/2/files/fileInfo" . $filename;

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


sub _astor_deleteFile
{
	my( $self, $filename) = @_;

	my $api_url = "/files" . $filename;
	
	my $response = $self->_astor_deleteRequest($api_url);
	if ( not defined $response )
	{
		$self->_log("_astor_deleteFile: Invalid response returned...");
		return;
	}
  
	return $response;
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


sub _astor_deleteRequest 
{
	my( $self, $url ) = @_;

	my $ark_server = $self->param( "server_url" );
	my $server_url = $ark_server . $url;
	
	my $ua       = LWP::UserAgent->new();
	my $req = HTTP::Request->new(DELETE => $server_url);
	my $response = $ua->request($req);

	return $response;
}


sub _set_debug {
	my ( $self, $enabled) = @_;
	
	my $repo = $self->{session}->get_repository;
	if ( $enabled ) {
		$repo->{noise} = 1;
	} else {
		$repo->{noise} = 0;
	}
}


sub _isDebug {
	my ( $self ) = @_;

	my $repo = $self->{session}->get_repository;

	return	$repo->{noise};
}

=back

=cut

1;

=head1 COPYRIGHT

=for COPYRIGHT BEGIN

Copyright 2013 Arkivum Limited

=for COPYRIGHT END

=for LICENSE BEGIN

This file is part of EPrints L<http://www.eprints.org/>.

EPrints is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

EPrints is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public
License along with EPrints.  If not, see L<http://www.gnu.org/licenses/>.

=for LICENSE END

