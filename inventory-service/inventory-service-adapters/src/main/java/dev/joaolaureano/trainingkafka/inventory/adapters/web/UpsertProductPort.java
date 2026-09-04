package dev.joaolaureano.trainingkafka.inventory.adapters.web;

/**
 * O que o adapter web precisa que alguém faça por ele.
 *
 * Declarada aqui, do lado de quem consome, e não do lado de quem implementa:
 * assim o controller compila sem conhecer o módulo -application.
 */
public interface UpsertProductPort {

    ProductResponse upsert(String sku, UpsertProductRequest request);
}
