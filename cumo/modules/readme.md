# Modules

## Cumulocity

Cumulocity modules allows to get data from the cumulocity platform and extract it for future use.

## Pipeline yaml structure

```
jobs:

- job: eco_counters
  schedule:
  m: 00
  h: 2
  active: true
  partition:
    type: &partition_type "daily"
    start: &partition_start "2025-02-16"
    elements:
    &elements ["BicycleIn", "BicycleOut", "PedestrianIn", "PedestrianOut"]

  assets:
  - asset: get_cumu_data
    group: cumu
    module:
      name: api
      action: get_api_data
      params:
      endpoint: https://tartu.platvorm.iot.telia.ee/measurement/measurements # it is possible to provide some parameters directly here
      auth:
      basic_auth:
        username: "{{ env.CUMO_USERNAME }}"
        password: "{{ env.CUMO_PASSWORD }}"
      partition_mapping: # This partition mapping allows to some partition values for injecting to the URL such as dates and elements/static
        elements: valueFragmentType # the value for fragment in the url
        dates: # only two options - dateFrom # the name of parameter in url for date from - dateTo # the name of parameter in URL for date to
      params: # parameters that should be used in the URL
      pageSize: 2000
      check_more: # check_more provide option to ask for another page if API can not return all required information via single request
        condition: "2000 == len(json_data['measurements'])" # condition for validation
        parameter: currentPage # parameters to send a get req to for another page

  - asset: json*mapper
    group: cumu
    ins: get_cumu_data
    module:
      name: json_mapper
      action: map_json
      params:
        mappings:
        source_id: measurements[*].source.id
        value: measurements[*]._.\_.value | [][]
        unit: measurements[_].\_.\_.unit | [][]
        time: measurements[*].time
        dal_series: measurements[*].keys(@)[-1]

  - asset: save_data
    group: cumu
    ins: json_mapper
    module:
      name: general
      action: write_to_csv
      params:
        file_name: "data/eco_counters/{date}/{static}.csv"
        minio:
          bucket: dagster-integration

```

### ins

Ins rely on Dagster I/O manager and provides a simplified way of moving data between assets. As a result this pipelines most of the time
allow to use only one ins value to the asset. This is because of the ins definition limitations. If you code need to have multiple ins from previous
tasks or other sources it is recommended to use dependencies.

### deps

Deps provides a dependency between different assets.

### Schedule

It is possible to define a schedule for a job. I it is working with both partitioned and non partitioned jobs. In a partition based job the following configuration should be used. For non partition the cron syntax should be used.

| **Parameter** | **Type** | **Required**           | **Default Value** | **Description**                             |
| ------------- | -------- | ---------------------- | ----------------- | ------------------------------------------- |
| `m`           | Integer  | Yes                    | None              | Minute of the execution                     |
| `h`           | Integer  | Yes                    | None              | Hour of the execution                       |
| `active`      | Bool     | No                     | None              | Should schedule be active or not by default |
| `cron`        | String   | Yes(non partition job) | None              | Schedule defined via cronjob syntax         |

Examples:

Partition job

```
schedule:
  m: 00
  h: 3
  active: true
```

At the moment active does not work as it should be.
Ordinary job

```
schedule:
  cron: *****
```

### Triggers

Triggers allow to run a job based on required conditions. To create triggers Dagster Sensor component is used.

Triggers are allowed only on job level. At the moment trigger simply triggers another job, if the previous was successful.

```
  triggers:
    - name: trigger_job_when_eco_counters_successful
      job: eco_counters
      state: successful
```

| **Parameter** | **Type** | **Required** | **Default Value** | **Description**                                                                        |
| ------------- | -------- | ------------ | ----------------- | -------------------------------------------------------------------------------------- |
| `name`        | String   | Yes          | None              | The name of the trigger. Should be valid python syntax for variable declaration. `job` |
| `start`       | String   | Yes          | None              | Name of the job, that will be checked.                                                 |
| `state`       | String   | No           | None              | It is not working properly now needed                                                  |

### Resources

The module relies on cumulocity resource.

```
resources:
  - module: cumo.resources.resources
    resource: Cumulocity
    name: cumulocity
    params:
      url: <cumulocity_address>
      username: env(CUMO_USERNAME)
      password: env(CUMO_PASSWORD)
```

### Defining different resources

### Partition

The module works only with partitions. It is mandatory to provide dates and elements for partition, where elements source ids of cumulocity measurements.

```
partition:
    type: "daily"
    start: "2024-07-20"
    end: "2024-07-21"
    elements:
    - "320669904"
```

| **Parameter** | **Type**     | **Required**                      | **Default Value** | **Description**                                                                                                            |
| ------------- | ------------ | --------------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `type`        | String       | required for time based partition | None              | Possible values are `hourly`, `daily`, `weekly`                                                                            |
| `start`       | String       | Yes                               | None              | Format of the date YYYY-mm-dd                                                                                              |
| `end`         | String       | No                                | None              | Should be set if partition will not be using schedules, set time boundary if needed                                        |
| `elements`    | List[String] | No                                | None              | Creates category based partition, can be used together with date partition, in this case a multi partition will be created |

## cumo.modules.cumulocity

### cumulocity

#### get_cumu_data

Modules gets date from the cumulocity API

| **Parameter** | **Type** | **Required** | **Default Value** | **Description**                                                                                                                                        |
| ------------- | -------- | ------------ | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `fragment`    | Boolean  | No           | False             | Sends API requests for fragments, where fragment name is the element from partition elements. Other vise uses name of the sources to send API requests |
| `page_size`   | Integer  | No           | 100               | How many measurements requests per page                                                                                                                |

### transform_cumu_data_to_dict_list

Module transforms cumulocity measurements to individual measurement with the next structure.

This modules reads inventory file from the filesystem by default. In a containerized environment S3 bucket should be used.

```
    "soource_id": <id of the source>,
    "date": <timestamp of the record>,
    "value": <measurement value>,
    "unit": <measurement unit>,
    "dal_type": <measurement fragment type>,
    "series_type":<measurement series type>,
    "source_name":inventory.get(m.source_id),
```

| **Parameter** | **Type** | **Required** | **Default Value** | **Description**                                               |
| ------------- | -------- | ------------ | ----------------- | ------------------------------------------------------------- | --- |
| `minio`       | Boolean  | No           | False             | Should be used if inventory configuration stored in S3 bucket |     |

### get_sources

Module extracts the name of sources from the get_cumu_data and transforms it to list of sources. Can be used to create a dynamic list of cumulocity sources if fragment option were used.

#### create_inventory

Creates inventory for provided sources. Can store data in S3 bucket.
| **Parameter** | **Type** | **Required** | **Default Value** | **Description** |
| ------------- | -------- | ------------ | ----------------- | --------------------------------------- |
| `sources` | List[String] | Yes | None | Id of sources |
| `minio` | Dict[String] | No | False | If True uses minio resource to upload configuration to the bucket |
| `minio.file_name` | String | Yes | None | Name of the file in S3 bucket|
| `minio.bucket` | String | Yes | None | Name of the S3 bucket |
It is possible to provide sources via ins.

## Arcgis

Module relies on arcgis resource

### transform_to_argcis_format

Modules transforms list of dictionaries to list of dictionaries that is supported by ArcGIS.

Module does not have any required or optional arguments

### send_to_arcgis

Modules sends data to arcgis.

Module relies on ArcGIS resource.
| **Parameter** | **Type** | **Required** | **Default Value** | **Description** |
| ----------------- | ------------ | ------------ | ----------------- | ----------------------------------------------------------------- |
| `create_sublayer` | Boolean | Yes | None |Creates sublayer if it does not exists in ArcGIS Feature server |
| `sublayer_name` | String | Yes | None | Name of the sublayer where data will be sent|
| `create_cols` | Boolean | Yes | None | Creates required columns in ArcGIS table if missing |
| `col_types` | Dict[Str] | Yes | None | List of columns that will be used in ArcGIS |
| `col_types.column` | String | None | esriFieldTypeNumber | Set column to required type in ArcGIS |

It is possible to provide sources via ins.

### correct_timestamps

Module corrects timestamps for each object in the list of dictionaries.

req timestamp_key

## general

### change_names

Changes value names in a Dataframe.

If includes only name the module will check is name present in the rows column and than replace with name substring.
It is possible to provide condition. Condition works the same way. If substring in the rows column it will be replaced with name.

| **Parameter**    | **Type**   | **Required** | **Default Value** | **Description**                                   |
| ---------------- | ---------- | ------------ | ----------------- | ------------------------------------------------- |
| `col`            | String     | Yes          | None              | Name of the column, where names should be changed |
| `names`          | List[Dict] | Yes          | None              |                                                   |
| `names.name`     | Boolean    | Yes          | None              | Desired name                                      |
| `name.condition` | Dict[Str]  | No           | None              | Which condition should be met                     |

Example

- name:chage -> "name to change" -> "change"

- name: new
  condition: change

  result: new

Example of params

```
            col: "source_name"
            names:
              - name: Anne
                contains: anne
              - name: Riia - Raudtee
              - name: Riia-Lembitu
              - name: Mõisavahe
                contains: mõisavahe
              - name: Turu
                contains: kl_turu
```

### remove_rows

Remove columns from the dataframe based on requirements

| **Parameter**    | **Type**   | **Required** | **Default Value** | **Description**                                   |
| ---------------- | ---------- | ------------ | ----------------- | ------------------------------------------------- |
| `col`            | String     | Yes          | None              | Name of the column, where names should be changed |
| `names`          | List[Dict] | Yes          | None              |                                                   |
| `names.name`     | Boolean    | Yes          | None              | Desired name                                      |
| `name.condition` | Dict[Str]  | No           | None              | Which condition should be met                     |

### rename_df_cols

Allows to rename dataframe columns

### df_to_dict

Covertes dataframe to list of dictinaries

### join_csvs

Module joins csvs on common attribute such as key. User can define the type of join and the key to use for this op.

| **Parameter**  | **Type**     | **Required** | **Default Value** | **Description**                                                   |
| -------------- | ------------ | ------------ | ----------------- | ----------------------------------------------------------------- |
| `file_names`   | List[String] | Yes          | None              | name of the CSV files                                             |
| `on`           | String       | Yes          | None              | Key for joining                                                   |
| `how`          | String       | Yes          | None              | Type of joining                                                   |
| `minio`        | Dict[String] | No           | False             | If True uses minio resource to upload configuration to the bucket |
| `minio.bucket` | String       | Yes          | None              | Name of the S3 bucket                                             |

### create_pivot

Creates pivot table based on users parameters

### aggregate

### pandas_ops

Modules allows to perform manipulation with the dataframe by using python based commands

```

```

### read_csv_pandas

# read_all:

# elements: \*elements # or maybe instead of key use element?>

# save: "data/eco_counters/clean/{date}.csv"

Reads CSV file directly to pandas Dataframe.
| **Parameter** | **Type** | **Required** | **Default Value** | **Description** |
| ------------- | -------- | ------------ | ----------------- | --------------------------------------- |
| `file_name` | String | Yes | None | name of the CSV file |
| `minio` | Dict[String] | No | False | If True uses minio resource to upload configuration to the bucket |
| `minio.bucket` | String | Yes | None | Name of the S3 bucket |
| `remove_duplicates` | Boolean | No | False | Removes all duplicates from the dataframe |
| `drop_cols` | List[String] | No | None | List of columns to drop|
| `remove_timeseries_duplicates` | Dict[Str] | No | False | Remove timeseries duplicates. rows of the same data,but different timestamp|
| `remove_timeseries_duplicates.value_col` | String | Yes | None | Name of the column with values|
| `remove_timeseries_duplicates.base_col` | String | Yes | None | Name of the column with timestamps or dates|
| `read_all` | Dict[Str] | No | None | Provides option to read all files from filesystem or s3|
| `read_all.elements` | List[Str] | No | None | keys for the file name add {key} to the file name. It will be fetched from this list|
| `read_all.save` | Str | No | None | File name where to save the merged version if needed. Pk keys are supported|

### write_to_csv

Writes data to CSV file in local store or saves CSV file to S3 bucket. Useful for cases where data should be joined or grouped before uploading to the destination.

| **Parameter**  | **Type**     | **Required** | **Default Value** | **Description**                                                                                                                                                                                                        |
| -------------- | ------------ | ------------ | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `file_name`    | String       | Yes          | None              | Name of the file. .csv not automatically added                                                                                                                                                                         |
| `add_pks`      | Boolean      | No           | False             | Allows to use partition keys in file naming. The next syntax should be used "{date}-{static}.csv", where data the starting date of partition and static the name of category, can be used together with multipartition |
| `minio`        | Dict[String] | No           | False             | If True uses minio resource to upload CSV files to the bucket                                                                                                                                                          |
| `minio.bucket` | String       | Yes          | None              | Name of the S3 bucket                                                                                                                                                                                                  |

file_name supports pks keys. It can use partitions values by using the next syntax in the configuration. "data/rattaringlus/{static}-{date}.csv"

### myrequests

#### http_get

Module allows to retrieve data via GET request. It supports basic, and token autht at the moment.

| **Parameter**                | **Type**            | **Required** | **Default Value** | **Description**                                                       |
| ---------------------------- | ------------------- | ------------ | ----------------- | --------------------------------------------------------------------- |
| `endpoint`                   | String              | Yes          | None              | name of the endpoint for get req                                      |
| `auth`                       | Auth method         | No           | None              | Auth option                                                           |
| `auth.basic_auth`            | -                   | No           | None              | -                                                                     |
| `auth.basic_auth.username`   | String              | Yes          | False             | Username for basic auth                                               |
| `auth.basic_auth.password`   | String              | Yes          | None              | Password for basic auth                                               |
| `auth.bearer_token`          | -                   | No           | None              | -                                                                     |
| `auth.bearer_token.token`    | String              | Yes          | False             | Token value for auth                                                  |
| `variables`                  | Dict[String]        | No           | None              | It is possible to define variable and after inject them to endpoint.  |
| `check_more`                 | String              | No           | None              | Password for basic auth                                               |
| `check_more.condition`       | Dict[String]        | Yes          | None              | Python condition that should be satisfied                             |
| `check_more.parameter`       | String              | Yes          | None              | Name of the parameter that will be used for page increase in endpoint |
| `partition_mapping`          | String              | No           | None              | Password for basic auth                                               |
| `partition_mapping.elements` | String              | No           | None              | Maps partition to provided parameter in endpoint                      |
| `partition_mapping.dates`    | List[String] Size 2 | No           | None              | Date partition start and Date partition end                           |
| `params`                     | Dict[String]        | No           | None              | Parameters that will be added to the endpoint                         |

#### variables example

```yaml
endpoint: https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}
variables:
  lat: 59.436962
  lon: 24.753574
```

#### check_more example

```yaml
check_more: # check_more provide option to ask for another page if API can not return all required information via single request
  condition: "2000 == len(json_data['measurements'])" # condition for validation
  parameter: currentPage # parameters to send a get req to for another page
```

#### partition_mapping example

```yaml
partition_mapping: # This partition mapping allows to some partition values for injecting to the URL such as dates and elements/static
  elements: "{{element_type | default('source')}}" # the value for fragment in the url
  dates: # only two options
    - dateFrom # the name of parameter in url for date from
    - dateTo # the name of parameter in URL for date to
```

### json_mapper

The module allows to work with list of JSON or JSON object directly

#### map_json

The module allows to map JSON values to a list of objects or to stand alone object.
It uses jmespath library for querying json object.

| **Parameter** | **Type**     | **Required** | **Default Value** | **Description**                                                                                                                                 |
| ------------- | ------------ | ------------ | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `mappings`    | Dict[String] | Yes          | None              | The key will be used as a key for new object and value a jmespath query                                                                         |
| `make_objs`   | Boolean      | No           | True              | Creates list of objects based on mapping results. It is useful for cases where many objects presents in the JSON like list of different objects |
|               |
| `direct`      | Boolean      | No           | False             | Returns direct response from json query for the first object only                                                                               |
|               |

Example

```
      - asset: extract_json
        ins: get_cumu_data
        module:
          name: json_mapper
          action: map_json
          params:
            mappings: # maps get json to required values
              unit: measurements[*].dal.series.unit
              value: measurements[*].dal.series.value
              time: measurements[*].time
```
