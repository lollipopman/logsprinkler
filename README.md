### Syslog sprinkler and a barrel to catch the sprinkles
Broadcasts UDP syslog messages to clients

### Technical
syslog-sprinkler is a daemon which listens for syslog messages on UDP port
5514. In addition it listens for clients, syslog-barrels, who are sending
heartbeats on UDP port 5515. For any syslog-barrels that are actively
heartbeating it broadcasts any syslog messages it is receiving on 5514 to those
active syslog-barrels.
