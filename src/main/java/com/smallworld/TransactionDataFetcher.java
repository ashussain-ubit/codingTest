package com.smallworld;

import com.smallworld.data.Transaction;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.math.BigDecimal;
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
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        BigDecimal totalAmount = transactions.stream()
                .map(Transaction::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        return totalAmount;
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public BigDecimal getTotalTransactionAmountSentBy(String senderFullName) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .filter(transaction -> transaction.getSenderFullName().equals(senderFullName))
                .collect(Collectors.toMap(
                        Transaction::getMtn,
                        Function.identity(),
                        (existing, replacement) -> existing))
                .values()
                .stream()
                .map(Transaction::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * Returns the highest transaction amount
     */
    public BigDecimal getMaxTransactionAmount() throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .map(Transaction::getAmount)
                .max(Comparator.naturalOrder())
                .orElse(null);
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .flatMap(transaction -> Stream.of(transaction.getSenderFullName(), transaction.getBeneficiaryFullName()))
                .distinct()
                .count();
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .filter(transaction -> transaction.getSenderFullName().equals(clientFullName) || transaction.getBeneficiaryFullName().equals(clientFullName))
                .anyMatch(transaction -> transaction.getIssueId() != 0 && !transaction.isIssueSolved());
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<String, List<Transaction>> getTransactionsByBeneficiaryName() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .collect(Collectors.groupingBy(Transaction::getBeneficiaryFullName));
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .filter(transaction -> transaction.getIssueId() != 0 && !transaction.isIssueSolved())
                .map(Transaction::getIssueId)
                .collect(Collectors.toSet());
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Transaction> transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});

        return transactions.stream()
                .filter(transaction -> transaction.getIssueId() != 0 && transaction.isIssueSolved())
                .map(Transaction::getIssueMessage)
                .collect(Collectors.toList());
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
