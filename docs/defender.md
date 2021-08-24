# Defender

The built-in `defender` allows you to configure an auto-blocking policy for SFTPGo and thus helps to prevent DoS (Denial of Service) and brute force password guessing.

If enabled it will protect SFTP, FTP and WebDAV services and it will automatically block hosts (IP addresses) that continually fail to log in or attempt to connect.

You can configure a score for each event type:

- `score_valid`, defines the score for valid login attempts, eg. user accounts that exist. Default `1`.
- `score_invalid`, defines the score for invalid login attempts, eg. non-existent user accounts or client disconnected for inactivity without authentication attempts. Default `2`.
- `score_limit_exceeded`, defines the score for hosts that exceeded the configured rate limits or the configured max connections per host. Default `3`.

And then you can configure:

- `observation_time`, defines the time window, in minutes, for tracking client errors.
- `threshold`, defines the threshold value before banning a host.
- `ban_time`, defines the time to ban a client, as minutes

So a host is banned, for `ban_time` minutes, if it has exceeded the defined threshold during the last observation time minutes.

A banned IP has no score, it makes no sense to accumulate host events in memory for an already banned IP address.

If an already banned client tries to log in again, its ban time will be incremented according the `ban_time_increment` configuration.

The `ban_time_increment` is calculated as percentage of `ban_time`, so if `ban_time` is 30 minutes and `ban_time_increment` is 50 the host will be banned for additionally 15 minutes. You can also specify values greater than 100 for `ban_time_increment` if you want to increase the penalty for already banned hosts.

The `defender` will keep in memory both the host scores and the banned hosts, you can limit the memory usage using the `entries_soft_limit` and `entries_hard_limit` configuration keys.

Using the REST API you can:

- list hosts within the defender's lists
- remove hosts from the defender's lists

The `defender` can also load a permanent block list and/or a safe list of ip addresses/networks from a file:

- `safelist_file`, defines the path to a file containing a list of ip addresses and/or networks to never ban.
- `blocklist_file`, defines the path to a file containing a list of ip addresses and/or networks to always ban.

These list must be stored as JSON conforming to the following schema:

- `addresses`, list of strings. Each string must be a valid IPv4/IPv6 address.
- `networks`, list of strings. Each string must be a valid IPv4/IPv6 CIDR address.

Here is a small example:

```json
{
    "addresses":[
        "192.0.2.1",
        "2001:db8::68"
    ],
    "networks":[
        "192.0.2.0/24",
        "2001:db8:1234::/48"
    ]
}
```

These list will be loaded in memory for faster lookups. The REST API queries "live" data and not these lists.

The `defender` is optimized for fast and time constant lookups however as it keeps all the lists and the entries in memory you should carefully measure the memory requirements for your use case.
