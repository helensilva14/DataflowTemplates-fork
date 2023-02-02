package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO.DynamicDataSourceConfiguration;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiBatchOptions;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import com.google.cloud.teleport.v2.templates.CustomBQJdbcToBigQuery.CustomBQJdbcToBQOptions;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.JdbcConverters;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
@Template(
    name = "BQ_Jdbc_to_BigQuery_Flex",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to BigQuery with BigQuery Storage API support",
    description =
        "A pipeline that reads from a JDBC source and writes to a BigQuery table. JDBC connection"
            + " string, user name and password can be passed in directly as plaintext or encrypted"
            + " using the Google Cloud KMS API.  If the parameter KMSEncryptionKey is specified,"
            + " connectionURL, username, and password should be all in encrypted format. A sample"
            + " curl command for the KMS API encrypt endpoint: curl -s -X POST"
            + " \"https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt\""
            + "  -d \"{\\\"plaintext\\\":\\\"PasteBase64EncodedString\\\"}\" -H \"Authorization:"
            + " Bearer $(gcloud auth application-default print-access-token)\" -H \"Content-Type:"
            + " application/json\"",
    optionsClass = CustomBQJdbcToBQOptions.class,
    flexContainerName = "bq-jdbc-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public class CustomBQJdbcToBigQuery {

  /** Interface used by the JdbcToBigQuery pipeline to accept user input. */
  public interface CustomBQJdbcToBQOptions
      extends CommonTemplateOptions, BigQueryStorageApiBatchOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        regexes = {"^.+$"},
        description = "Cloud Storage paths for JDBC drivers",
        helpText = "Comma separate Cloud Storage paths for JDBC drivers.",
        example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
    String getDriverJars();

    void setDriverJars(String driverJar);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        regexes = {"^.+$"},
        description = "JDBC driver class name.",
        helpText = "JDBC driver class name to use.",
        example = "com.mysql.jdbc.Driver")
    String getDriverClassName();

    void setDriverClassName(String driverClassName);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        regexes = {
            "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
        },
        description = "JDBC connection URL string.",
        helpText =
            "Url connection string to connect to the JDBC source. Connection string can be passed in"
                + " as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.",
        example = "jdbc:mysql://some-host:3306/sampledb")
    String getConnectionURL();

    void setConnectionURL(String connectionURL);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
        description = "JDBC connection property string.",
        helpText =
            "Properties string to use for the JDBC connection. Format of the string must be"
                + " [propertyName=property;]*.",
        example = "unicode=true;characterEncoding=UTF-8")
    String getConnectionProperties();

    void setConnectionProperties(String connectionProperties);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        regexes = {"^.+$"},
        description = "JDBC connection username.",
        helpText =
            "User name to be used for the JDBC connection. User name can be passed in as plaintext "
                + "or as a base64 encoded string encrypted by Google Cloud KMS.")
    String getUsername();

    void setUsername(String username);

    @TemplateParameter.Password(
        order = 6,
        optional = true,
        description = "JDBC connection password.",
        helpText =
            "Password to be used for the JDBC connection. Password can be passed in as plaintext "
                + "or as a base64 encoded string encrypted by Google Cloud KMS.")
    String getPassword();

    void setPassword(String password);

    @TemplateParameter.Text(
        order = 7,
        optional = false,
        regexes = {"^.+$"},
        description = "JDBC source SQL query.",
        helpText = "Query to be executed on the source to extract the data.",
        example = "select * from sampledb.sample_table")
    String getQuery();

    void setQuery(String query);

    @TemplateParameter.BigQueryTable(
        order = 8,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The name should be in the format"
                + " <project>:<dataset>.<table_name>. The table's schema must match input objects.")
    String getOutputTable();

    void setOutputTable(String value);

    @TemplateParameter.GcsWriteFolder(
        order = 9,
        optional = false,
        description = "Temporary directory for BigQuery loading process",
        helpText = "Temporary directory for BigQuery loading process",
        example = "gs://your-bucket/your-files/temp_dir")
    String getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(String directory);

    @TemplateParameter.KmsEncryptionKey(
        order = 10,
        optional = true,
        description = "Google Cloud KMS key",
        helpText =
            "If this parameter is provided, password, user name and connection string should all be"
                + " passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See:"
                + " https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt",
        example = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
    String getKMSEncryptionKey();

    void setKMSEncryptionKey(String keyName);

    @TemplateParameter.Boolean(
        order = 11,
        optional = true,
        description = "Whether to use column alias to map the rows.",
        helpText =
            "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the"
                + " column name to map the rows to BigQuery. Defaults to false.")
    @Default.Boolean(false)
    Boolean getUseColumnAlias();

    void setUseColumnAlias(Boolean useColumnAlias);

    @TemplateParameter.Boolean(
        order = 12,
        optional = true,
        description = "Whether to truncate data before writing",
        helpText =
            "If enabled (set to true) the pipeline will truncate before loading data into BigQuery."
                + " Defaults to false, which is used to only append data.")
    @Default.Boolean(false)
    Boolean getIsTruncate();

    void setIsTruncate(Boolean isTruncate);

    @TemplateParameter.BigQueryTable(
        order = 13,
        description = "BigQuery input table",
        helpText =
            "BigQuery table location to read the input data. The name should be in the format"
                + " <project>:<dataset>.<table_name>.")
    String getInputTable();

    void setInputTable(String value);
  }


  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link JdbcToBigQuery#run} method to start the pipeline
   * and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line
    CustomBQJdbcToBQOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomBQJdbcToBQOptions.class);

    run(options, writeToBQTransform(options));
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @param writeToBQ the transform that outputs {@link TableRow}s to BigQuery.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(CustomBQJdbcToBQOptions options, Write<TableRow> writeToBQ) {
    // Validate BQ STORAGE_WRITE_API options
    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Step 1: Read records from existing BigQuery table (PCollection<TableRow>)
     */
    PCollection<KV<String, TableRow>> bqData = pipeline
        .apply("Read from input BigQuery table",
            BigQueryIO.readTableRows()
            .withoutValidation()
            .from(options.getInputTable()))
        .apply("Convert to KV<dept_no, TableRow>",
            WithKeys.of((SerializableFunction<TableRow, String>) row -> (String)row.get("dept_no")));

    /*
     * Step 2: Read records via JDBC and convert to TableRow
     *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
     */
    PCollection<KV<String, TableRow>> mySqlData = pipeline
        .apply("Read from JdbcIO", DynamicJdbcIO.<TableRow>read()
            .withDataSourceConfiguration(
                DynamicDataSourceConfiguration.create(options.getDriverClassName(),
                    maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
                .withUsername(maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
                .withPassword(maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
                .withDriverJars(options.getDriverJars())
                .withConnectionProperties(options.getConnectionProperties())
            )
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow(options.getUseColumnAlias()))
        )
        .apply("Convert to KV<dept_no, TableRow>",
            WithKeys.of((SerializableFunction<TableRow, String>) row -> (String)row.get("dept_no")));;

    final TupleTag<TableRow> bqDataTag = new TupleTag<>();
    final TupleTag<TableRow> mySqlDataTag = new TupleTag<>();

    // Merge collection values into a CoGbkResult collection.
    PCollection<KV<String, CoGbkResult>> joinedCollection =
        KeyedPCollectionTuple
            .of(bqDataTag, bqData)
            .and(mySqlDataTag, mySqlData)
            .apply(CoGroupByKey.create());

    //joinedCollection.apply(MapElements.into(TypeDescriptors.strings()));

    //joinedCollection.apply(TextIO.write().to("result.txt"));

        /*
         * Step 2: Append TableRow to an existing BigQuery table
         */
     //   .apply("Write to BigQuery", writeToBQ);

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Create the {@link Write} transform that outputs the collection to BigQuery as per input option.
   */
  @VisibleForTesting
  static Write<TableRow> writeToBQTransform(CustomBQJdbcToBQOptions options) {
    return BigQueryIO.writeTableRows()
        .withoutValidation()
        .withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(
            options.getIsTruncate()
                ? Write.WriteDisposition.WRITE_TRUNCATE
                : Write.WriteDisposition.WRITE_APPEND)
        .withCustomGcsTempLocation(
            StaticValueProvider.of(options.getBigQueryLoadingTemporaryDirectory()))
        .to(options.getOutputTable());
  }
}
