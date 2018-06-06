# Mask filter plugin for Embulk

[![Coverage Status](https://coveralls.io/repos/github/beniyama/embulk-filter-mask/badge.svg)](https://coveralls.io/github/beniyama/embulk-filter-mask)

Mask columns with asterisks in a variety of patterns (still in initial development phase and missing basic features to use in production).

## Overview

* **Plugin type**: filter

## Configuration

*Caution* : Now we use `type` to specify mask types such as `all` and `email`, instead of `pattern` which was used in version 0.1.1 or earlier.

- **columns**: target columns which would be replaced with asterisks (string, required)
  - **name**: name of the column (string, required)
  - **type**: mask type, `all`, `email`, `regex` or `substring` (string, default: `all`)
  - **paths**: list of JSON path and type, works if the column type is JSON
    - `[{key: $.json_path1}, {key: $.json_path2}]` would mask both `$.json_path1` and `$.json_path2` nodes
    - Elements under the nodes would be converted to string and then masked (e.g., `[0,1,2]` -> `*******`)
  - **length**: if specified, this filter replaces the column with fixed number of asterisks (integer, optional. supported only in `all`, `email`, `substring`.)
  - **pattern**: Regex pattern such as "[0-9]+" (string, required for `regex` type)
  - **start**: The beginning index for `substring` type. The value starts from 0 and inclusive (integer, default: 0)
  - **end**: The ending index for `substring` type. The value is exclusive (integer, default: length of the target column)

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
      - { name: contact, type: email, length: 5}
```

would produce

|first_name | last_name | gender | age | contact |
|---|---|---|---|---|
| Benjamin | **** | male | ** | *****@example.com |
| Lucas | ****** | male | ** | *****@example.com |
| Elizabeth |	*** | female | ** | *****@example.com |
| Christian | **** | male | ** | *****@example.com |
| Amy |	***** | female | ** | *****@example.com |

If you use `regex` and/or `substring` types,

```yaml
filters:
  - type: mask
    columns:
      - { name: first_name, type: regex, pattern: "[a-z]"}
      - { name: contact, type: substring, start: 5, length: 5}
```

would produce

|first_name | last_name | gender | age | contact |
|---|---|---|---|---|
| B******* | Bell | male | 30 | bell.***** |
| L**** | Duncan | male | 20 | lucas***** |
| E******* |	May | female | 25 | eliza***** |
| C******** | Reid | male | 15 | chris***** |
| A** |	Avery | female | 40 | amy.a***** |

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
      - { name: user, paths: [{key: $.full_name.first_name}, {key: $.email, type: email}]}    
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
