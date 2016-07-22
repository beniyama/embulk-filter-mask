# Mask filter plugin for Embulk

mask columns with asterisks (still in initial development phase and missing basic functionalities to use in production )

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: target columns which would be replaced with asterisks (string, required)
  - **name**: name of the column (string, required)
  - **pattern**: mask pattern, `all` or `email` (string, default: `all`)
  - **path**: JSON path, works if the column type is JSON (string, default: `$.`)

## Example

If you have below data in csv or other format file,

|first_name | last_name | gender | age | contact |
|---|---|---|---|---|
| Benjamin | Bell | male | 30 | bell.benjamin_dummy@<i></i>example.com |
| Lucas | Duncan | male | 20 | lucas.duncan_dummy@<i></i>example.com |
| Elizabeth |	May | female | 25 | elizabeth.may_dummy@<i></i>example.com |
| Christian | Reid | male | 15 | christian.reid_dummy@<i></i>example.com |
| Amy |	Avery | female | 40 | amy.avercy_dummy@<i></i>example.com |

below filter configuration

```yaml
filters:
  - type: mask
    columns:
      - { name: last_name}
      - { name: age}
      - { name: contact, pattern: email}
```

would produce

|first_name | last_name | gender | age | contact |
|---|---|---|---|---|
| Benjamin | **** | male | ** | *****@example.com |
| Lucas | ****** | male | ** | *****@example.com |
| Elizabeth |	*** | female | ** | *****@example.com |
| Christian | **** | male | ** | *****@example.com |
| Amy |	***** | female | ** | *****@example.com |

JSON type column is also partially supported.

If you have

```json
{
  "full_name": {
    "first_name": "Benjamin",
    "last_name": "Bell"
  },
  "gender": "male",
  "age": 30
}
```

below filter configuration

```yaml
filters:
  - type: mask
    columns:
      - { name: full_name, path: $.first_name}
      - { name: age, path: $.}      
```

would produce

```json
{
  "full_name": {
    "first_name": "********",
    "last_name": "Bell"
  },
  "gender": "male",
  "age": **
}
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
