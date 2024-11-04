package edu.smu.smusql.interfaces;
import edu.smu.smusql.parser.*;
import java.util.List;

public interface StorageInterface {

    boolean tableExists(String tableName);
    void insert(Insert insert);
    List<String> getColumns(String tableName);
    int getColumnCount(String tableName);
    int delete(Delete delete);
    List<RowEntry> select(Select select);
    int update(Update update);
    void create(Create create);
}
