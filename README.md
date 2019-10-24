# Logsprinkler

Broadcasts UDP syslog messages to clients

## Description

logsprinkler is a daemon which listens for syslog messages on UDP port 5515. In
addition it listens for clients, logbarrels, who are sending heartbeats on UDP
port 5514. For any logbarrels that are actively heartbeating it broadcasts any
syslog messages it is receiving on 5515 to those active logbarrels on 5514.

# Build

```
# build individual programs
$ (cd logsprinkler; go build)
$ (cd logbarrel; go build)

# or build a Debian package
$ dpkg-buildpackage -us -uc -b -d
```
