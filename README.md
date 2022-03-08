# rk-cache
Cache entries with backend service.

## Supported backend

| Name           | Dependency                                             | Example                  |
|----------------|--------------------------------------------------------|--------------------------|
| rk-cache       | Local memory cache                                     | [example](redis/example) |
| rk-cache/redis | [go-redis/cache](https://github.com/go-redis/cache/v8) | [example](redis/example) |