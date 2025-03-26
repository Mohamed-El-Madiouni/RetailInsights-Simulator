provider "aws" {
  region = var.aws_region
}

resource "aws_glue_catalog_database" "retail_db" {
  name = var.glue_database_name
  description = "Catalogue des données Retail Insights"
}

# Création tables
resource "aws_glue_catalog_table" "clients" {
  name          = "clients"
  database_name = aws_glue_catalog_database.retail_db.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "${var.s3_base_path}/clients"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
    columns {
      name = "age"
      type = "int"
    }
    columns {
      name = "gender"
      type = "string"
    }
    columns {
      name = "loyalty_card"
      type = "boolean"
    }
    columns {
      name = "city"
      type = "string"
    }
  }

  parameters = {
    "classification" = "parquet"
  }
}

resource "aws_glue_catalog_table" "products" {
  name          = "products"
  database_name = aws_glue_catalog_database.retail_db.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "${var.s3_base_path}/products"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
    columns {
      name = "category"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "cost"
      type = "double"
    }
  }

  parameters = {
    "classification" = "parquet"
  }
}

resource "aws_glue_catalog_table" "stores" {
  name          = "stores"
  database_name = aws_glue_catalog_database.retail_db.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "${var.s3_base_path}/stores"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "name"
      type = "string"
    }
    columns {
      name = "location"
      type = "string"
    }
    columns {
      name = "capacity"
      type = "int"
    }
    columns {
      name = "opening_hour"
      type = "int"
    }
    columns {
      name = "closing_hour"
      type = "int"
    }
  }

  parameters = {
    "classification" = "parquet"
  }
}

resource "aws_glue_catalog_table" "sales" {
  name          = "sales"
  database_name = aws_glue_catalog_database.retail_db.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "${var.s3_base_path}/sales/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "sale_id"
      type = "string"
    }
    columns {
      name = "nb_type_product"
      type = "int"
    }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "client_id"
      type = "string"
    }
    columns {
      name = "store_id"
      type = "string"
    }
    columns {
      name = "quantity"
      type = "int"
    }
    columns {
      name = "sale_amount"
      type = "double"
    }
    columns {
      name = "sale_time"
      type = "string"
    }
  }

  partition_keys {
    name = "sale_date"
    type = "date"
  }

  parameters = {
    "classification" = "parquet"
  }
}

resource "aws_glue_catalog_table" "retail_data" {
  name          = "retail_data"
  database_name = aws_glue_catalog_database.retail_db.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "${var.s3_base_path}/retail_data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "store_id"
      type = "string"
    }
    columns {
      name = "store_name"
      type = "string"
    }
    columns {
      name = "hour"
      type = "int"
    }
    columns {
      name = "visitors"
      type = "int"
    }
    columns {
      name = "sales"
      type = "int"
    }
  }

  partition_keys {
    name = "date"
    type = "date"
  }

  parameters = {
    "classification" = "parquet"
  }
}
