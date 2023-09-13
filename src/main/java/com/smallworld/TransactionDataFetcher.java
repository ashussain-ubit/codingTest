package com.smallworld;

import com.smallworld.data.Transaction;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionDataFetcher {


    /**
     * Returns the sum of the amounts of all transactions
     */
    public BigDecimal getTotalTransactionAmount() throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        BigDecimal totalAmount = new BigDecimal(0);
        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>(){});
                totalAmount = totalAmount.add(transactions.stream()
                        .map(Transaction::getAmount)
                        .reduce(BigDecimal.ZERO, BigDecimal::add));
            }
        }
        return totalAmount;
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public BigDecimal getTotalTransactionAmountSentBy(String senderFullName) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        BigDecimal totalAmount = new BigDecimal(0);

        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>(){});
                totalAmount = totalAmount.add(transactions.stream()
                        .filter(transaction -> transaction.getSenderFullName().equals(senderFullName))
                        .collect(Collectors.toMap(
                                Transaction::getMtn,
                                Function.identity(),
                                (existing, replacement) -> existing))
                        .values()
                        .stream()
                        .map(Transaction::getAmount)
                        .reduce(BigDecimal.ZERO, BigDecimal::add));
            }
        }
        return totalAmount;
    }

    /**
     * Returns the highest transaction amount
     */
    public BigDecimal getMaxTransactionAmount() throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        BigDecimal macTransactionAmount = new BigDecimal(0);

        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>() {});
                macTransactionAmount = macTransactionAmount.add(transactions.stream()
                        .map(Transaction::getAmount)
                        .max(Comparator.naturalOrder())
                        .orElse(null));
            }
        }
        return macTransactionAmount;
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        long uniqueClientsCount = 0;

        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>() {});
                uniqueClientsCount = uniqueClientsCount + transactions.stream()
                        .flatMap(transaction -> Stream.of(transaction.getSenderFullName(), transaction.getBeneficiaryFullName()))
                        .distinct()
                        .count();
            }
        }
        return uniqueClientsCount;
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        boolean hasOpenIssue = false;

        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>() {});
                hasOpenIssue = transactions.stream()
                        .filter(transaction -> transaction.getSenderFullName().equals(clientFullName) || transaction.getBeneficiaryFullName().equals(clientFullName))
                        .anyMatch(transaction -> transaction.getIssueId() != 0 && !transaction.isIssueSolved());
            }
        }
        return hasOpenIssue;
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<String, List<Transaction>> getTransactionsByBeneficiaryName() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<Transaction>> map = new HashMap<>();

        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>() {});
                Map<String, List<Transaction>> tempMap = transactions.stream()
                        .collect(Collectors.groupingBy(Transaction::getBeneficiaryFullName));
                tempMap.forEach((key, value) ->
                        map.merge(key, value, (existingList, newList) -> {
                            existingList.addAll(newList);
                            return existingList;
                        })
                );
            }
        }
        return map;
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Set<Integer> unsolvedIssueIds = new HashSet<>();
        Path directory = Paths.get(System.getProperty("user.dir"));

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>() {});
                Set<Integer> tempSet = transactions.stream()
                        .filter(transaction -> transaction.getIssueId() != 0 && !transaction.isIssueSolved())
                        .map(Transaction::getIssueId)
                        .collect(Collectors.toSet());
                unsolvedIssueIds.addAll(tempSet);
            }
        }
        return unsolvedIssueIds;
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<String> unsolvedMessages = new ArrayList<>();

        Path directory = Paths.get(System.getProperty("user.dir"));
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, "*.json")) {
            for (Path path : directoryStream) {
                String content = new String(Files.readAllBytes(path));
                List<Transaction> transactions = mapper.readValue(content, new TypeReference<List<Transaction>>() {});
                List<String> tempList = transactions.stream()
                        .filter(transaction -> transaction.getIssueId() != 0 && transaction.isIssueSolved())
                        .map(Transaction::getIssueMessage)
                        .collect(Collectors.toList());
                unsolvedMessages.addAll(tempList);
            }
        }
        return unsolvedMessages;
    }

    /**
     * Returns the 3 transactions with the highest amount sorted by amount descending
     */
    public List<Transaction> getTop3TransactionsByAmount() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .collect(Collectors.toMap(
                        Transaction::getAmount,
                        Function.identity(),
                        (existing, replacement) -> existing
                ))
                .values()
                .stream()
                .sorted(Comparator.comparing(Transaction::getAmount).reversed())
                .limit(3)
                .collect(Collectors.toList());
    }

    /**
     * Returns the senderFullName of the sender with the most total sent amount
     */
    public Optional<String> getTopSender() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .collect(Collectors.toMap(
                        Transaction::getMtn,
                        Function.identity(),
                        (existing, replacement) -> existing
                ))
                .values()
                .stream()
                .collect(Collectors.groupingBy(Transaction::getSenderFullName,
                        Collectors.reducing(BigDecimal.ZERO, Transaction::getAmount, BigDecimal::add)))
                .entrySet()
                .stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

}
