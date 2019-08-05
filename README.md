# poediscordbackend
Backend section of the bot that does most of the work.

Running:
====
Create a config file in `./instance/` named `config.json` and/or `config_testing.json`. The _testing config file is read when running unit tests.

The file currently has the following form:
```json
{
    "broker": {
        "type": "redis",
        "uri": "redis://myserver.example.com/0"
    }
}
```

Before every test we flushdb() so make sure to use a different database number in your testing config.