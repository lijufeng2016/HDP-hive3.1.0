package org.apache.hadoop.hive.metastore.tools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class SchemaToolTaskMergeCatalog extends SchemaToolTask {
  private static final Logger
      LOG = LoggerFactory.getLogger(org.apache.hadoop.hive.metastore.tools.SchemaToolTaskMergeCatalog.class.getName());

  private String fromCatalog;
  private String toCatalog;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    fromCatalog = normalizeIdentifier(cl.getOptionValue("mergeCatalog"));
    toCatalog = cl.getOptionValue("toCatalog");
  }

  private static final String DB_CONFLICTS_STMT =
      "SELECT d.<q>NAME<q> as DB, d.<q>CTLG_NAME<q>, d2.<q>CTLG_NAME<q> FROM <q>DBS<q> d, <q>DBS<q> d2 "
          + "WHERE d.<q>NAME<q> = d2.<q>NAME<q> AND "
          + "d.<q>CTLG_NAME<q> = '%s' AND d2.<q>CTLG_NAME<q> = '%s'";

  private static final String MERGE_CATALOG_STMT =
      "UPDATE <q>DBS<q> " +
          " SET <q>CTLG_NAME<q> = '%s' " + " WHERE <q>CTLG_NAME<q> = '%s'";

  private static final String CONVERT_TABLE_TO_EXTERNAL =
      "update <q>TBLS<q> t set t.<q>TBL_TYPE<q> = '%s' where t.<q>TBL_ID<q> in (" +
      "select <q>TBL_ID<q> from <q>TBLS<q> t2, <q>DBS<q> d where t2.<q>TBL_TYPE<q> = '%s' and t2.<q>DB_ID<q> = d.<q>DB_ID<q> " +
      "and d.<q>CTLG_NAME<q> = '%s') ";

  private static final String UPDATE_CTLG_NAME_ON_DBS =
      "update <q>DBS<q> d set d.<q>CTLG_NAME<q> = '%s' WHERE d.<q>CTLG_NAME<q> = '%s' ";

  private static final String ADD_PARAM_TO_TABLE =
      "INSERT INTO <q>TABLE_PARAMS<q> (<q>TBL_ID<q>, <q>PARAM_KEY<q>, <q>PARAM_VALUE<q>) select <q>TBL_ID<q>, "
          + "'%s', '%s' from <q>TBLS<q> where <q>TBL_TYPE<q> = '%s' ";

  private static final String ADD_AUTOPURGE_TO_TABLE =
      "INSERT INTO <q>TABLE_PARAMS<q> (<q>TBL_ID<q>, <q>PARAM_KEY<q>, <q>PARAM_VALUE<q>) select <q>TBL_ID<q>, "
          + "'%s', '%s' from <q>TBLS<q> t, <q>DBS<q> d, <q>CTLGS<q> c "
          + "where <q>TBL_TYPE<q> = '%s' and t.<q>DB_ID<q> = d.<q>DB_ID<q> and d.<q>CTLG_NAME<q> = c.<q>NAME<q> and c.<q>NAME<q> = '%s' ";

  @Override
  void execute() throws HiveMetaException {
    if (fromCatalog == null || toCatalog == null) {
      throw new HiveMetaException("Merge catalog requires --mergeCatalog and --toCatalog arguments");
    }
    System.out.println("Merging databases from " + fromCatalog + " to " + toCatalog);

    Connection conn = schemaTool.getConnectionToMetastore(true);
    boolean success = false;
    long initTime, prevTime, curTime;

    try {
      // determine conflicts between catalogs first
      try (Statement stmt = conn.createStatement()) {
        initTime = System.currentTimeMillis();
        // TODO ensure both catalogs exist first.

        // Detect conflicting databases
        String conflicts = String.format(schemaTool.quote(DB_CONFLICTS_STMT), fromCatalog, toCatalog);
        System.out.println("Determining name conflicts between databases across catalogs");
        LOG.info("[DB Conflicts] Executing SQL:" + conflicts);
        ResultSet rs = stmt.executeQuery(conflicts);
        boolean cleanMerge = true;
        while (rs.next()) {
          cleanMerge = false;
          System.out.println(
              "Name conflict(s) between merging catalogs, database " + rs.getString(1) + " exists in catalogs "
                  + rs.getString(2) + " and " + rs.getString(3));
        }

        if (!cleanMerge) {
          System.out.println("[ERROR] Please resolve the database name conflicts shown above manually and retry the mergeCatalog operation.");
          System.exit(1);
        }

        conn.setAutoCommit(false);
        String insert =
            String.format(schemaTool.quote(ADD_AUTOPURGE_TO_TABLE), "EXTERNAL", "TRUE", "MANAGED_TABLE", fromCatalog);
        System.out.println("Setting external=true on all MANAGED tables in catalog " + fromCatalog);
        LOG.debug("[external table property] Executing SQL:" + insert);
        prevTime = System.currentTimeMillis();
        int count = stmt.executeUpdate(insert);
        curTime = System.currentTimeMillis();
        System.out.println("Set external.table.purge on " + count + " tables, time taken (ms):" + (curTime - prevTime));

        insert = String.format(schemaTool.quote(ADD_AUTOPURGE_TO_TABLE), "external.table.purge", "true", "MANAGED_TABLE",
            fromCatalog);
        System.out.println("Setting external.table.purge=true on all MANAGED tables in catalog " + fromCatalog);
        LOG.debug("[external.table.purge] Executing SQL:" + insert);
        prevTime = curTime;
        count = stmt.executeUpdate(insert);
        curTime = System.currentTimeMillis();
        System.out.println("Set external.table.purge on " + count + " tables, time taken (ms):" + (curTime - prevTime));

        String update =
            String.format(schemaTool.quote(CONVERT_TABLE_TO_EXTERNAL), "EXTERNAL_TABLE", "MANAGED_TABLE", fromCatalog);
        System.out.println("Setting tableType to EXTERNAL on all MANAGED tables in catalog " + fromCatalog);
        LOG.debug("[tableType=EXTERNAL_TABLE] Executing SQL:" + update);
        prevTime = curTime;
        count = stmt.executeUpdate(update);
        curTime = System.currentTimeMillis();
        System.out.println("Set tableType=EXTERNAL_TABLE on " + count + " tables, time taken (ms):" + (curTime - prevTime));

        String merge = String.format(schemaTool.quote(MERGE_CATALOG_STMT), toCatalog, fromCatalog);
        System.out.println("Setting catalog names on all databases in catalog " + fromCatalog);
        LOG.debug("[catalog name] Executing SQL:" + merge);
        prevTime = curTime;
        count = stmt.executeUpdate(merge);
        curTime = System.currentTimeMillis();
        System.out.println("Changed catalog names on " + count + " databases, time taken (ms):" + (curTime - prevTime));

        if (count == 0) {
          LOG.info(count + " databases have been merged from catalog " + fromCatalog + " into " + toCatalog);
        }
        if (schemaTool.isDryRun()) {
          conn.rollback();
          success = true;
        } else {
          conn.commit();
          System.out.println("Committed the changes. Total time taken (ms):" + (curTime - initTime));
          success = true;
        }
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to merge catalog", e);
    } finally {
      try {
        if (!success) {
          System.out.println("Rolling back transaction");
          conn.rollback();
        }
        conn.close();
      } catch (SQLException e) {
        // Not really much we can do here.
        LOG.error("Failed to rollback, everything will probably go bad from here.", e);
        try {
          conn.close();
        } catch (SQLException ex) {
          LOG.warn("Failed to close connection.", ex);
        }
      }
    }
  }
}
