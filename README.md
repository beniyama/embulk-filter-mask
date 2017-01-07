# Mask filter plugin for Embulk

mask columns with asterisks (still in initial development phase and missing basic functionalities to use in production )

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: target columns which would be replaced with asterisks (string, required)
  - **name**: name of the column (string, required)
  - **pattern**: mask pattern, `all` or `email` (string, default: `all`)
  - **paths**: list of JSON path and pattern, works if the column type is JSON
    - `[{key: $.json_path1}, {key: $.json_path2}]` would mask both `$.json_path1` and `$.json_path2` nodes
    - Elements under the nodes would be converted to string and then masked (e.g., `[0,1,2]` -> `*******`)
  - **length**: if specified, this filter replaces the column with fixed number of asterisks (integer, optional)

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
      - { name: contact, pattern: email, length: 5}
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

If you have a `user` column with this JSON data structure

```json
{
  "full_name": {
    "first_name": "Benjamin",
    "last_name": "Bell"
  },
  "gender": "male",
  "age": 30,
  "email": "test_mail@example.com"
}
```

below filter configuration

```yaml
filters:
  - type: mask
    columns:
      - { name: user, paths: [{key: $.full_name.first_name}, {key: $.email, pattern: email}]}    
```

would produce

```json
{
  "full_name": {
    "first_name": "********",
    "last_name": "Bell"
  },
  "gender": "male",
  "age": 30,
  "email": "*********@example.com"
}
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
