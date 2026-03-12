<title> TF2 Log Search Results </title>
<link href="favicon.ico" rel="icon" type="image/x-icon" />
<?php
$steamid = $_POST['steamid'];
$word = $_POST['word'];
$a = "/usr/bin/python3 /srv/http/tf2/Tf2LogSearcher/database/test.py \"$word\" \"$steamid\"";
echo passthru($a);
$protocol = $_SERVER['SERVER_PROTOCOL'];
$ip = $_SERVER['REMOTE_ADDR'];
$port = $_SERVER['REMOTE_PORT'];
$agent = $_SERVER['HTTP_USER_AGENT'];
$ref = $_SERVER['HTTP_REFERER'];
$hostname = gethostbyaddr($_SERVER['REMOTE_ADDR']);

//Print IP, Hostname, Port Number, User Agent and Referer To Log.TXT

$fh = fopen('chatlog.txt', 'a');
date_default_timezone_set("America/Chicago");
$date = new DateTime();
$date = $date->format("m/d/Y h:i:s A e");
fwrite($fh, 'Timestamp: '."".$date ."\n");
fwrite($fh, 'IP Address: '."".$ip ."\n");
fwrite($fh, 'Hostname: '."".$hostname ."\n");
fwrite($fh, 'Port Number: '."".$port ."\n");
fwrite($fh, 'User Agent: '."".$agent ."\n");
fwrite($fh, 'HTTP Referer: '."".$ref ."\n");
fwrite($fh, 'Word: '."".$word ."\n");
fwrite($fh, 'SteamID: '."".$steamid ."\n\n");
fclose($fh);
?>
</body>
</html>
