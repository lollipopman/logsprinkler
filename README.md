### logsprinkler and logbarrels to catch the sprinkles
Broadcasts UDP syslog messages to clients

### Technical
logsprinkler is a daemon which listens for syslog messages on UDP port
5515. In addition it listens for clients, logbarrels, who are sending
heartbeats on UDP port 5514. For any logbarrels that are actively
heartbeating it broadcasts any syslog messages it is receiving on 5515 to those
active logbarrels on 5514.
