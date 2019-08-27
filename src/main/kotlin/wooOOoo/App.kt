package wooOOoo

import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import com.fasterxml.jackson.databind.*
import io.ktor.jackson.*
import io.ktor.request.*
import io.ktor.routing.*
import java.util.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction


object Movies : Table() {
    val id = integer("id").autoIncrement().primaryKey() // Column<Int>
    val title = varchar("name", 50) // Column<String>
}

class MovieBody(val title : String, val id : Int, val time : String?)

class MovieWithShowings(val title: String, val id : Int, val times : List<String>)

class MovieResponse(val title: String, val id : Int)

class MoviePayload(val title: String)

class ShowingsPayload(val time : String, val movieId : Int)

object Showings : Table() {
    val time = varchar("time", 50)
    val movieId = (integer("movieId") references Movies.id)
}

fun Application.module() {
    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
        }
    }

    Database.connect("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")

    transaction {
        SchemaUtils.create(Movies, Showings)
    }

    install(Routing) {
        get("/movies") {
            var movies = listOf<MovieWithShowings>()
            transaction {
                movies = (Movies leftJoin Showings)
                .selectAll().map {
                    MovieBody(it[Movies.title], it[Movies.id], it[Showings.time])
                }.fold(mapOf<Int, MovieWithShowings>()) { acc, current ->
                    val movie = acc.getOrDefault(current.id, MovieWithShowings(current.title, current.id, listOf()) )
                    val newMovie = MovieWithShowings(
                        movie.title,
                        movie.id,
                        if(current.time != null) { movie.times.plus(current.time) } else { movie.times }
                    )
                    acc.plus(current.id to newMovie)
                }.map{ it.value }
            }
            call.respond(movies)
        }
        post("/movies") {
            val body = call.receive<MoviePayload>()

            var id = 0
            transaction {
                id = Movies.insert {
                    it[title] = body.title
                } get Movies.id
            }
            call.respond(MovieResponse(body.title, id))
        }

        post("/showings") {
            val body = call.receive<ShowingsPayload>()

            transaction {
                Showings.insert {
                    it[time] = body.time
                    it[movieId] = body.movieId
                }
            }
    
            call.respond(body)
        }
    }
}

fun main(args: Array<String>) {
    embeddedServer(Netty, 8080, watchPaths = listOf("AppKt"), module = Application::module).start()
}