package dev.joaolaureano.trainingkafka.analytics.domain.port;

import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductId;
import dev.joaolaureano.trainingkafka.analytics.domain.model.ProductSalesRecord;

import java.util.List;

/**
 * Port de saída para o agregado {@link ProductSalesRecord}.
 *
 * Nenhuma palavra aqui diz SQL, tabela, conexão ou arquivo. Trocar SQLite por
 * DuckDB, ou por um mapa em memória, não muda uma vírgula desta interface — nem
 * do domínio que a usa. Se algum dia uma implementação precisar mudar a FORMA
 * deste contrato para caber na tecnologia dela, o contrato é que está errado.
 */
public interface ProductSalesRepository {

    /**
     * Devolve o acumulado do produto, ou um registro zerado se for a primeira venda.
     * Nunca devolve {@code null} nem {@code Optional} vazio: do ponto de vista do
     * domínio, todo produto sempre teve um acumulado — só que às vezes era zero.
     */
    ProductSalesRecord findOrCreate(ProductId productId);

    void save(ProductSalesRecord record);

    /**
     * Ranking por unidades vendidas, decrescente.
     *
     * Esta consulta vive no repositório, e não no agregado, porque é comparativa:
     * um agregado não enxerga além da própria fronteira e não pode saber sua
     * posição relativa aos outros.
     */
    List<ProductSalesRecord> topSelling(int limit);
}
