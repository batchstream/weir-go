# Connections

How a Client finds servers, keeps connections healthy and closes.

## Address and reconnection

Point a client at a Weir application address and send typed business requests to
the Stores hosted there. IP and DNS addresses work in Kubernetes and other
deployments.

`New` takes a `host:port` address and rejects anything else. It connects on
first use and reconnects after failures. When a DNS name resolves to several
Weir servers, streams are spread round-robin across them, so every server behind
the address must host the Stores the client uses. A Read or Mutate of one
request usually shares streams, and any other call uses a gRPC stream of its own,
as described under [Streaming and deadlines](streaming.md). Use a name
that lists every server, such as a Kubernetes headless Service; a ClusterIP
Service resolves to one virtual IP. The Client looks the name up again every 5
seconds. A server that joins receives new calls once connected, and shared
streams move to it until every server carries about the same number. A server
that stops resolving, as a Kubernetes Pod does once it starts terminating,
receives no new records; its streams finish the records they carry and close.
So scaling a headless Service up or down, or rolling out a new version, does not
fail calls, however long they run, provided a server stops resolving at least a
few seconds before it stops taking records, as the Weir chart's preStop delay
arranges. A server that does not host the requested
Store fails the call with NotFound.

## Reuse and close

Reuse the client across goroutines and
close it after callers finish.
Calls started after `Close` return `ErrClosed`; calls still running end with a
gRPC cancellation error. A call whose context ends returns the context's
error, so test it with `errors.Is` rather than its gRPC status. A Client sends a record again only when a server refused it
before it began, as described under
[Resending refused records](streaming.md#resending-refused-records).

## Kubernetes DNS names

In Kubernetes, give the Service's full name with a trailing dot, such as
`weir.application.svc.cluster.local.:7447`. Without the dot, a name with fewer
than five dots is first tried under each of the Pod's DNS search domains. In a
Pod with four search domains, one lookup of `weir.application.svc` sends 6
queries and `weir.application.svc.cluster.local` sends 10, against 2 with the
dot; every Client repeats its lookup every 5 seconds.

## Keepalive

A Client pings a server it has heard nothing from for 10 seconds and drops the
connection if no answer comes within 5 seconds, so calls leave a server they
can no longer reach, such as one behind a broken network path or a hung
process, even while its name still resolves. Calls waiting on that connection
end with an error, and a mutation sent on it has an unknown outcome. Servers
must accept pings this often, as those of the Weir chart 0.15.0-rc.1 and later
do; an older server ends such connections, after which the Client pings less
often for the rest of its life. Upgrade servers before Clients.

## Network access

The Weir application listener uses plaintext gRPC; restrict its network access.
