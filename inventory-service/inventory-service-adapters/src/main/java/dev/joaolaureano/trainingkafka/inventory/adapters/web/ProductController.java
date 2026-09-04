package dev.joaolaureano.trainingkafka.inventory.adapters.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Adapter de entrada HTTP do catálogo.
 *
 * Recebe, delega pelos Ports que ele mesmo declara e traduz o resultado. Não
 * decide nada — se aparecer regra de negócio aqui, ela vazou de dentro.
 */
@RestController
@RequestMapping("/products")
public class ProductController {

    private final UpsertProductPort upsertProduct;
    private final FindProductPort findProduct;

    public ProductController(UpsertProductPort upsertProduct, FindProductPort findProduct) {
        this.upsertProduct = upsertProduct;
        this.findProduct = findProduct;
    }

    /**
     * PUT, e não POST: o SKU identifica o recurso e vem na URL, então mandar a
     * mesma requisição duas vezes deixa o catálogo no mesmo estado. É o que um
     * seed de carga precisa — e o que um POST com quantidade acumulada não daria.
     */
    @PutMapping("/{sku}")
    public ResponseEntity<ProductResponse> upsert(@PathVariable String sku,
                                                  @RequestBody UpsertProductRequest request) {
        return ResponseEntity.ok(upsertProduct.upsert(sku, request));
    }

    @GetMapping("/{sku}")
    public ResponseEntity<ProductResponse> bySku(@PathVariable String sku) {
        return findProduct.bySku(sku)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<ProductResponse> all() {
        return findProduct.all();
    }
}
