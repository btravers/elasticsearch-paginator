package elasticsearchpaginator.core.model

import java.math.BigInteger
import java.security.MessageDigest

data class Query(
        val index: String,
        val query: String,
        val sort: String,
        val firstPageSize: Int?,
        val size: Int
) {

    fun hash(): String {
        val messageDigest = MessageDigest.getInstance("MD5")
        return BigInteger(1, messageDigest.digest(this.toString().toByteArray()))
                .toString(16)
                .padStart(32, '0')
    }

}
