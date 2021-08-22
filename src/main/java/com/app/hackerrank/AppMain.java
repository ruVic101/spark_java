package com.app.hackerrank;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class AppMain {
    private static final String accountsPath = "src/main/resources/accounts.csv";
    private static final String transactionsPath = "src/main/resources/transactions.csv";

    private static Dataset<Row> extractValidTransactions(Dataset<Row> accountsDf, Dataset<Row> transactionsDf ){

        Dataset<Row> totalAggregatedTransaction = transactionsDf.select("from_account_number","amount")
                .groupBy("from_account_number").agg(sum(col("amount")).as("total_amount"));

        totalAggregatedTransaction.printSchema();

        totalAggregatedTransaction.show();

        // Validate Transactions

        Dataset<Row> validatedTxnsAcc = accountsDf.join(totalAggregatedTransaction,
                totalAggregatedTransaction.col("from_account_number")
                        .equalTo(accountsDf.col("account_number")),"inner")
                .filter(col("balance").$greater$eq(col("total_amount"))).select("account_number");

        validatedTxnsAcc.show();
        Dataset<Row> validatedTxns = transactionsDf.join(validatedTxnsAcc, transactionsDf.col("from_account_number")
                .equalTo(validatedTxnsAcc.col("account_number")),"inner").select(transactionsDf.col("*"));
        validatedTxns.show();

        return validatedTxns;
    }

    private static Long distinctTransactions(Dataset<Row> transactionsDS){
        return transactionsDS.select("from_account_number").distinct().count();
    }

    private static Map<String,Long> transactionByAccount(Dataset<Row> transactionDf){

        transactionDf.groupBy(col("from_account_number"))
                .agg(count("from_account_number").as("per_account_count")).show();

        Object[] abc = transactionDf.groupBy(col("from_account_number"))
                .agg(count("from_account_number").as("per_account_count"))
        .javaRDD().map(r -> r.getString(0)+"~"+r.getLong(1)).collect().toArray();

        Map<String,Long> mapOut = new HashMap<String, Long>();
        for (Object i:abc) {
            String[] keyAndVal = i.toString().split("~");
            String key = keyAndVal[0];
            Long val = (long) Integer.parseInt(keyAndVal[1]);
            mapOut.put(key,val);
        }
        return mapOut;
    }

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf= new SparkConf().setAppName("Java Spark").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println(spark);
        // Define Schema
        StructType accountsSchema = new StructType()
                .add("account_number","string")
                .add("balance","int");
        StructType transactionsSchema = new StructType()
                .add("from_account_number","string")
                .add("to_account_number","string")
                .add("amount","int");

        Dataset<Row> accountsDf = spark.read().schema(accountsSchema).csv(accountsPath);
        Dataset<Row> transactionsDf = spark.read().schema(transactionsSchema).csv(transactionsPath);

        accountsDf.show();
        transactionsDf.show();

        extractValidTransactions(accountsDf,transactionsDf).show();
        System.out.println(distinctTransactions(transactionsDf));
        System.out.println(transactionByAccount(transactionsDf));

    }
}