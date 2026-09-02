package dev.joaolaureano.trainingkafka.audit.adapters.persistence.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.joaolaureano.trainingkafka.audit.domain.model.ApplicationName;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditEvent;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditFilter;
import dev.joaolaureano.trainingkafka.audit.domain.model.AuditLevel;
import dev.joaolaureano.trainingkafka.audit.domain.port.AuditRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementação DuckDB (JDBC puro) de {@link AuditRepository}.
 *
 * A conexão é injetada e permanece de responsabilidade de quem a criou — este
 * adapter nunca a abre nem a fecha.
 */
public class DuckDbAuditRepository implements AuditRepository {

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS audit_events (
                occurred_at_millis BIGINT  NOT NULL,
                level              VARCHAR NOT NULL,
                app                VARCHAR NOT NULL,
                action             VARCHAR NOT NULL,
                context_json       VARCHAR NOT NULL
            )
            """;

    private final Connection connection;
    private final ObjectMapper mapper = new ObjectMapper();

    public DuckDbAuditRepository(Connection connection) {
        this.connection = connection;
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("falha ao criar a tabela logs no DuckDB", e);
        }
    }

    @Override
    public void save(AuditEvent entry) {
        // Logs são append-only: nunca há UPDATE aqui, só INSERT.
        String sql = "INSERT INTO audit_events (occurred_at_millis, level, app, action, context_json) "
                + "VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, entry.occurredAt().toEpochMilli());
            statement.setString(2, entry.level().name());
            statement.setString(3, entry.app().value());
            statement.setString(4, entry.action());
            statement.setString(5, writeContext(entry.context()));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("falha ao gravar registro de log no DuckDB para " + entry.app(), e);
        }
    }

    @Override
    public List<AuditEvent> query(AuditFilter filter, int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT occurred_at_millis, level, app, action, context_json FROM audit_events WHERE 1 = 1");
        List<Object> params = new ArrayList<>();

        filter.minimumLevelOrEmpty().ifPresent(minimum -> {
            // A coluna guarda texto, mas o filtro é por severidade MÍNIMA, não igualdade:
            // pedir WARN precisa trazer WARN e ERROR também. Traduzimos isso para um
            // "level IN (...)" com os níveis que satisfazem o mínimo.
            List<AuditLevel> accepted = Arrays.stream(AuditLevel.values())
                    .filter(level -> level.isAtLeast(minimum))
                    .toList();
            String placeholders = accepted.stream().map(l -> "?").collect(Collectors.joining(", "));
            sql.append(" AND level IN (").append(placeholders).append(")");
            accepted.forEach(level -> params.add(level.name()));
        });

        filter.appOrEmpty().ifPresent(app -> {
            sql.append(" AND app = ?");
            params.add(app.value());
        });

        filter.rangeOrEmpty().ifPresent(range -> {
            sql.append(" AND occurred_at_millis >= ? AND occurred_at_millis <= ?");
            params.add(range.start().toEpochMilli());
            params.add(range.end().toEpochMilli());
        });

        sql.append(" ORDER BY occurred_at_millis DESC LIMIT ?");
        params.add(limit);

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = statement.executeQuery()) {
                List<AuditEvent> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(toAuditEvent(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new RuntimeException("falha ao consultar logs no DuckDB para o filtro " + filter, e);
        }
    }

    private AuditEvent toAuditEvent(ResultSet rs) throws SQLException {
        return new AuditEvent(
                AuditLevel.parse(rs.getString("level")),
                Instant.ofEpochMilli(rs.getLong("occurred_at_millis")),
                new ApplicationName(rs.getString("app")),
                rs.getString("action"),
                readContext(rs.getString("context_json"))
        );
    }

    private String writeContext(Map<String, String> context) {
        try {
            return mapper.writeValueAsString(context);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("falha ao serializar contexto do log para JSON", e);
        }
    }

    private Map<String, String> readContext(String json) {
        try {
            if (json == null || json.isBlank()) {
                return Map.of();
            }
            return mapper.readValue(json, new TypeReference<LinkedHashMap<String, String>>() { });
        } catch (Exception e) {
            throw new RuntimeException("falha ao desserializar contexto do log a partir de JSON: " + json, e);
        }
    }
}
