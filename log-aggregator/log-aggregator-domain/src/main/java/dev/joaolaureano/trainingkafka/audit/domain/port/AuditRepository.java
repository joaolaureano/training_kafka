package dev.joaolaureano.trainingkafka.audit.domain.port;

import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;

import java.util.List;

/**
 * Port de saída para os registros de log.
 *
 * Nenhuma menção a arquivo, tabela, console ou conexão — as três implementações
 * (stdout, arquivo JSONL e DuckDB) são radicalmente diferentes entre si, e ainda
 * assim cabem nesta mesma interface.
 */
public interface AuditRepository {

    void save(AuditEvent entry);

    /**
     * Registros que atendem ao filtro, do mais recente para o mais antigo.
     *
     * <p><strong>Nem toda implementação consegue responder.</strong> Um adapter que
     * só imprime no console não guarda nada, e portanto devolve lista vazia — não
     * porque nada casou, mas porque não há onde procurar. Isso está documentado
     * aqui, e cada implementação nessa situação repete o aviso, para que a lista
     * vazia nunca seja confundida com "não há logs".
     */
    List<AuditEvent> query(AuditFilter filter, int limit);
}
