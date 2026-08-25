package dev.joaolaureano.trainingkafka.logs.domain.port;

import dev.joaolaureano.trainingkafka.logs.domain.model.LogEntry;
import dev.joaolaureano.trainingkafka.logs.domain.model.LogFilter;

import java.util.List;

/**
 * Port de saída para os registros de log.
 *
 * Nenhuma menção a arquivo, tabela, console ou conexão — as três implementações
 * (stdout, arquivo JSONL e DuckDB) são radicalmente diferentes entre si, e ainda
 * assim cabem nesta mesma interface.
 */
public interface LogRepository {

    void save(LogEntry entry);

    /**
     * Registros que atendem ao filtro, do mais recente para o mais antigo.
     *
     * <p><strong>Nem toda implementação consegue responder.</strong> Um adapter que
     * só imprime no console não guarda nada, e portanto devolve lista vazia — não
     * porque nada casou, mas porque não há onde procurar. Isso está documentado
     * aqui, e cada implementação nessa situação repete o aviso, para que a lista
     * vazia nunca seja confundida com "não há logs".
     */
    List<LogEntry> query(LogFilter filter, int limit);
}
