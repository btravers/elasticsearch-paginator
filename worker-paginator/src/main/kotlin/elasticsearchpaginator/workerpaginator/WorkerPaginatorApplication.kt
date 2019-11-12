package elasticsearchpaginator.workerpaginator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class WorkerPaginatorApplication

fun main(args: Array<String>) {
    runApplication<WorkerPaginatorApplication>(*args)
}
