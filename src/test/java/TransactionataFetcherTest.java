import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smallworld.TransactionDataFetcher;
import com.smallworld.data.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TransactionataFetcherTest {

    List<Transaction> transactions;
    TransactionDataFetcher transactionDataFetcher;

    @Before
    public void setUp() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        transactionDataFetcher = new TransactionDataFetcher();
        transactions = mapper.readValue(new File("transactions.json"), new TypeReference<List<Transaction>>(){});
    }

    @Test
    public void shouldReturnTotalAmountOfAllTransactions() throws Exception {
        BigDecimal totalAmount = transactionDataFetcher.getTotalTransactionAmount();
        assertEquals(BigDecimal.valueOf(4371.37), totalAmount);
    }

    @Test
    public void shouldReturnTotalTransactionAmountSentBy() throws Exception {
        BigDecimal totalAmount = transactionDataFetcher.getTotalTransactionAmountSentBy("Tom Shelby");
        assertEquals(BigDecimal.valueOf(678.06), totalAmount);
    }

    @Test
    public void shouldReturnMaxTransactionAmount() throws Exception {
        BigDecimal totalAmount = transactionDataFetcher.getMaxTransactionAmount();
        assertEquals(BigDecimal.valueOf(985.0), totalAmount);
    }

    @Test
    public void shouldReturnCountOfUniqueClients() throws Exception {
        long totalAmount = transactionDataFetcher.countUniqueClients();
        assertEquals(14, totalAmount);
    }

    @Test
    public void shouldReturnHasOpenComplianceIssuesOfProvidedClientName() throws Exception {
        boolean hasOpenIssue = transactionDataFetcher.hasOpenComplianceIssues("Grace Burgess");
        assertEquals(true, hasOpenIssue);
    }

    @Test
    public void shouldReturnTransactionsByBeneficiaryName() throws Exception {
        Set<Integer> expectedSet = new HashSet<>(Arrays.asList(1, 3, 15, 54, 99));
        Map<String, List<Transaction>> transactionsByBenificary = transactionDataFetcher.getTransactionsByBeneficiaryName();
        assertEquals(3, transactionsByBenificary.get("Michael Gray").size());
        assertEquals(1, transactionsByBenificary.get("MacTavern").size());
        assertEquals(2, transactionsByBenificary.get("Arthur Shelby").size());
        assertEquals(1, transactionsByBenificary.get("Ben Younger").size());
    }

    @Test
    public void shouldReturnUnsolvedIssueIds() throws Exception {
        Set<Integer> expectedSet = new HashSet<>(Arrays.asList(1, 3, 15, 54, 99));
        Set<Integer> openIssueIds = transactionDataFetcher.getUnsolvedIssueIds();
        assertEquals(expectedSet, openIssueIds);
    }

    @Test
    public void shouldReturnAllSolvedIssueMessages() throws Exception {
        List<String> expectedSet = Arrays.asList("Never gonna give you up", "Never gonna let you down", "Never gonna run around and desert you");
        List<String> issueMessages = transactionDataFetcher.getAllSolvedIssueMessages();
        assertEquals(expectedSet, issueMessages);
    }

    @Test
    public void shouldReturnTop3TransactionsByAmount() throws Exception {
        List<Transaction> transactions = transactionDataFetcher.getTop3TransactionsByAmount();
        assertEquals(5465465, transactions.get(0).getMtn());
        assertEquals(32612651, transactions.get(1).getMtn());
        assertEquals(663458, transactions.get(2).getMtn());
    }

    @Test
    public void shouldReturnTopSender() throws Exception {
        Optional<String> senderName = transactionDataFetcher.getTopSender();
        assertEquals("Arthur Shelby", senderName.get());
    }


}
