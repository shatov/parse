#!/usr/bin/perl
use strict;
use DBI;
use Time::HiRes qw(gettimeofday tv_interval);

my $t0 = [gettimeofday];

# File to parse
my $filename = 'out';

# Connect to the database.
my $host     = 'localhost';
my $dbase    = 'maillog';
my $user     = 'logger';
my $password = 'pa77wor1';


my $dbh     = DBI->connect("DBI:mysql:database=$dbase;host=$host", $user, $password,
                    { RaiseError => 1, mysql_server_prepare => 1 });
my $sth_mes = $dbh->prepare("INSERT INTO message (created, id, int_id, str) VALUES (?, ?, ?, ?)");
my $sth_log = $dbh->prepare("INSERT INTO log (created, int_id, str, address) VALUES (?, ?, ?, ?)");

open ( my $fh, "<", $filename ) or die "Can't open < $filename: $!";

# String variants
#2012-02-13 14:59:15 1Rwtcp-0000Ac-BW <= tpxmuwr@somehost.ru H=mail.somehost.com [84.154.134.45] P=esmtp S=1486 id=120213145115.FAXCHADMNIC.888573@whois.somehost.ru
#2012-02-13 14:59:15 1Rwtcp-0000Ac-BW == pmtzjnyta@brightconsult.ru R=dnslookup T=remote_smtp defer (-1): domain matches queue_smtp_domains, or -odqs set
#2012-02-13 15:02:15 1Rwtcp-0000Ac-BW Spool file is locked (another process is handling this message)
#2012-02-13 15:02:27 1Rwtcp-0000Ac-BW ** pmtzjnyta@brightconsult.ru R=dnslookup T=remote_smtp: SMTP error from remote mail server after RCPT TO:<pmtzjnyta@brightconsult.ru>: host tmg.navicons.ru [86.110.12.210]: 550 5.7.1 :127.0.0.5:Client host 194.226.74.6 blocked using 88.blocklist.zap; Mail from IP banned. To request removal from this list please forward this message to dfrvhdkjrrrkkqjpv@messaging.microsoft.com
#2012-02-13 15:02:28 1Rwtfv-0002ry-W4 <= <> R=1Rwtcp-0000Ac-BW U=mailnull P=local S=2984
#2012-02-13 15:02:28 1Rwtfv-0002ry-W4 => :blackhole: <tpxmuwr@somehost.ru> R=blackhole_router
#2012-02-13 15:02:28 1Rwtfv-0002ry-W4 Completed
#2012-02-13 15:02:51 SMTP connection from [109.70.26.4] (TCP/IP connection count = 1)
my $count = 0;
while ( my $line = <$fh> ) {
    chomp $line;
    my ( $data, $time, $str, $int_id, $flag, $other ) = $line =~ /^
        (.*?)\s       # $1 data
        (.*?)\s       # $2 time
        (                               # $3 str
            (.{6}-.{6}-.{2} | )         # $4 int_id or nothing
            \s?                     
            ( <= | => | -> | \*\* | == |  )  # $5 flag or nothing
            \s?
            (.*$)                       # $6 other
        )/x;
    
    my $created = "$data $time";
    if ( $flag eq '<=' ) {
        my ( $id_mes ) = $str =~ /id=(.*?)(\s|$)/;
        
        if ( $id_mes ) {
            # message table
            $sth_mes->execute( $created, $id_mes, $int_id, $str );
        }
        else {
            # log table ( created, int_id, str, address )
            $sth_log->execute( $created, $int_id, $str, undef );
        }
    }
    else {
        my ( $address ) = $str =~ /(?: <= | => | -> | \*\* | ==) \s (\S+? \@ \S+? \. \S+?) (?: \s | :)/x;        
        $sth_log->execute( $created, $int_id, $str, $address );
    }
}

close $fh;
$dbh->disconnect();

print "Parsing time: " . tv_interval ( $t0, [gettimeofday] ) . "\n";

1;