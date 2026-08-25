package dev.joaolaureano.trainingkafka.analytics.domain.model;

/**
 * Resultado de uma consulta de faturamento por período.
 *
 * Value object puro: não tem identidade, não recebe eventos, nunca é mutado.
 * É <em>derivado</em> por agregação sobre o ledger de pedidos, jamais armazenado
 * — guardá-lo criaria uma segunda verdade sobre o mesmo fato.
 */
public record RevenueWindow(TimeRange range, Money total, long orderCount) {

    public RevenueWindow {
        if (range == null) {
            throw new InvalidValueException("range é obrigatório");
        }
        if (total == null) {
            throw new InvalidValueException("total é obrigatório");
        }
        if (orderCount < 0) {
            throw new InvalidValueException("orderCount não pode ser negativo");
        }
    }

    public static RevenueWindow empty(TimeRange range) {
        return new RevenueWindow(range, Money.ZERO, 0);
    }

    /** Ticket médio do período. Sem pedidos, é zero — e não uma divisão por zero. */
    public Money averageTicket() {
        return total.dividedBy(orderCount);
    }
}
