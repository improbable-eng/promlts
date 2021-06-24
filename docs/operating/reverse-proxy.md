# Running Thanos behind a reverse proxy

There are many reasons to use a [reverse proxy](https://www.nginx.com/resources/glossary/reverse-proxy-server/) in front of Thanos, for example, TLS termination (serve Thanos over HTTPS) and basic authentication. This small guide will tell you how to do that.

## External Prefix and Route Prefix

Before continuing, let's first take a look at two CLI flags provided by Thanos, `--web.external-prefix` and `--web.route-prefix`. The external prefix is useful when you want to prefix all requests from the UI with some path. Normally, the web UI would load all static assets from `/static/...`, do API calls on `/api/v1/...` etc. But if we use `--web.external-prefix="thanos"` the UI would prefix each request with `/thanos`. It would try to load all assets from `/thanos/static/...`, do API calls on `/thanos/api/v1/...` and so on. One thing to note here is that `--web.external-prefix` only prefixes the requests and redirects with the specified value, but Thanos is still listening on the root, not the specified sub-path i.e. the API is still accessible at `/api/v1/...` and not at `/thanos/api/v1`. This is where `--web.route-prefix` comes in. If you set `--web.route-prefix="thanos"` every route would get prefixed with the specified value. For example, the API will be accessible on `/thanos/api/v1`. As this is the most common use case when using the `--web.external-prefix`, the default value of `--web.route-prefix` is the value of `--web.external-prefix`.

Note: Using different values for `--web.external-prefix` and `--web.route-prefix` can lead to the web UI not working properly if it is accessed directly (without a reverse proxy).

## Examples

Let's look into some example scenarios. All examples are using nginx as a reverse proxy here.

### Serving Thanos on a subdomain

Serving a Thanos component on the root of a subdomain is pretty straight-forward. Let's say you want to run Thanos Querier behind a nginx reverse proxy, accessible on domain `thanos.example.com`. A basic nginx configuration would look like this:

```
http {
    server {
        listen 80;
        server_name thanos.example.com;

        location / {
            proxy_pass    http://localhost:10902/;
        }
    }
}
```

### Serving Thanos on a sub-path

Things become a little tricky when you want to serve Thanos on a sub-path. Let's say, you want to run Thanos Querier behind an nginx server, accessible on the URL `http://example.com/thanos`. The Thanos web UI depends on it being accessed on the same URL as Thanos itself is listening. This is because the UI needs to know the URL from where to load static assets and what URL to use in links or redirects. If Thanos is behind a reverse proxy, particularly one where Thanos is not at the root, this doesn't work so well.

To tackle this problem, Thanos provides a flag `--web.external-prefix`.

Let's say we have Thanos Querier running on the usual port, we need nginx running with the following configuration:

```
http {
    server {
        listen 80;
        server_name example.com;

        location /thanos/ {
            proxy_pass    http://localhost:10902/thanos/;
        }
    }
}
```

With this configuration, you can access Thanos Querier on `http://example.com/thanos`. Notice that because we are using `http://localhost:10902/thanos/` as the reverse proxy target, every request path will be prefixed with `/thanos`. To make this work we need to run Thanos Querier like this:

```
thanos query --web.external-prefix="thanos"
```

It should be noted that now the `/thanos` path prefix would be required for all HTTP access to Thanos. For example, the `/metrics` endpoint would be accessible at `http://localhost:10902/thanos/metrics`.
