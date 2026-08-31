# ElasticSearch Committer

Elasticsearch implementation of Norconex Committer.

Website: https://opensource.norconex.com/committers/elasticsearch/

## Maven coordinates

This committer uses its own Maven group id, distinct from every other V3
Norconex module:

```xml
<dependency>
  <groupId>com.norconex.collectors.v3-align</groupId>
  <artifactId>norconex-committer-elasticsearch</artifactId>
  <version>3.2.0</version>
</dependency>
```

Why: prior releases of this committer were numbered independently and had
already reached `5.0.0` (own numbering, unrelated to any Elasticsearch
version) by the time every other V3 module was synchronized onto a single
shared version. Publishing `3.2.0` under the same coordinates as `5.0.0`
would have looked like a downgrade, and tools resolving "latest" would keep
returning `5.0.0`. The `v3-align` group id marks this as the version-synced
release line for this committer, while `com.norconex.collectors:
norconex-committer-elasticsearch` remains at `5.0.0` for anyone still on it.

## Compatibility Matrix

| This Committer      | Committer Core | HTTP Collector | FS Collector |
| -------------------- | -------------- | -------------- | ------------ |
| **4.x**               | 2.x            | 2.x            | 2.x          |
| **5.x**               | 3.x            | 3.x            | -            |
| **3.2.0+** (master)  | 3.x            | 3.x            | -            |
