extern crate cdrs_tokio_helpers_derive;

use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::frame::TryFromRow;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::prelude::Row;
use cdrs_tokio::types::IntoRustByName;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use actix_cors::Cors;
use serde::{Deserialize, Serialize};
use serde_json::json;
use once_cell::sync::OnceCell;
use uuid::Uuid;

type CurrentSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

static SESSION: OnceCell<CurrentSession> = OnceCell::new();

async fn initialize_session() -> CurrentSession {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .build()
        .await
        .expect("Cluster configuration failed");

    let session = TcpSessionBuilder::new(
        RoundRobinLoadBalancingStrategy::new(),
        cluster_config,
    )
    .build()
    .await
    .expect("Session creation failed");

    session
}

#[derive(Deserialize)]
struct DeleteData{
    id: String
}

#[derive(Deserialize, Serialize)]
struct CreateData{
    title: String,
    description: String,
    timestamp: i64
}

#[derive(Debug, Serialize, Deserialize)]
struct TodoData{
    id: String,
    title: String,
    description: String,
    timestamp: i64,
    status: bool
}

impl TryFromRow for TodoData {
    fn try_from_row(row: Row) -> Result<Self, cdrs_tokio::error::Error> {
        let id: Uuid = row
            .get_by_name("id")
            .map_err(|e| format!("Failed to get id: {:?}", e))?
            .ok_or_else(|| "Id is null".to_string())?;
        
        let title: String = row
            .get_by_name("title")
            .map_err(|e| cdrs_tokio::error::Error::from(e))?
            .ok_or_else(|| cdrs_tokio::error::Error::from("Title is null".to_string()))?;

        let description: String = row
            .get_by_name("description")
            .map_err(|e| cdrs_tokio::error::Error::from(e))?
            .ok_or_else(|| cdrs_tokio::error::Error::from("Description is null".to_string()))?;

        let timestamp: i64 = row
            .get_by_name("insertion_time")
            .map_err(|e| cdrs_tokio::error::Error::from(e))?
            .ok_or_else(|| cdrs_tokio::error::Error::from("Timestamp is null".to_string()))?;

        let status: bool = row
            .get_by_name("status")
            .map_err(|e| cdrs_tokio::error::Error::from(e))?
            .ok_or_else(|| cdrs_tokio::error::Error::from("Status is null".to_string()))?;

        // Convert Uuid to String
        let id_str = id.to_string();

        Ok(TodoData {
            id: id_str,
            title,
            description,
            timestamp,
            status,
        })
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let session = initialize_session().await;
    SESSION.set(session).expect("Failed to set global session");
    println!("Server running at http://127.0.0.1:3001");

    // Start HTTP server and pass session as shared state
    HttpServer::new(move || {
        let cors = Cors::default()
        .allow_any_origin()
        .allow_any_header()
        .allow_any_method()
        .expose_any_header();

        App::new()
            .wrap(cors)
            .service(ks_init)
            .service(read_data)
            .service(delete_data)
            .service(create_data)
            .service(update_data)
            // .route("/query", web::post().to(query))
    })
    .bind("127.0.0.1:3001")?
    .run()
    .await
}

#[get("/")]
async fn ks_init() -> impl Responder{
    let session = SESSION.get().expect("Session not initialized");

    let create_ks = "CREATE KEYSPACE IF NOT EXISTS todo_app WITH REPLICATION = { \
                     'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

    let create_tb = "CREATE TABLE IF NOT EXISTS todo_app.lists (id UUID PRIMARY KEY, \
                     insertion_time TIME, title TEXT, description TEXT, status BOOLEAN);";

    session
        .query(create_ks)
        .await
        .expect("Keyspace creation error");

    session
        .query(create_tb)
        .await
        .expect("Table creation error");

    HttpResponse::Ok().body("Keyspace & Table created successfully!")
}

#[get("/read")]
async fn read_data() -> impl Responder{

    let session = SESSION.get().expect("Session not initialized");

    let read_query = "SELECT * FROM todo_app.lists";
    
    let rows = session.query(read_query).await.expect("query").response_body().expect("body").into_rows().expect("into rows");

    let mut response_rows: Vec<TodoData> = Vec::new();

    for row in rows{
        let my_row: TodoData = TodoData::try_from_row(row).expect("into TodoData");
        println!("struct got: {my_row:?}");
        response_rows.push(my_row);
    };

    let json_response = serde_json::to_string(&response_rows).expect("Failed to serialize to JSON");

    HttpResponse::Ok().content_type("application/json").body(json_response)
}

#[post("/delete")]
async fn delete_data(data: web::Json<DeleteData>) -> impl Responder{

    let session = SESSION.get().expect("Session not initialized");

    let delete_query = format!("DELETE FROM todo_app.lists WHERE id={} IF EXISTS;", data.id);

    match session.query(delete_query).await {
        Ok(result) => match result.response_body() {
            Ok(_) => {
                let response = json!({ "status": "Sucess", "message": "Successfully delete requested id" });

                HttpResponse::Ok().content_type("application/json").json(response)
            },
            Err(e) => {
                let response = json!({ "status": "Failed", "message": format!("Failed to process response body : {}", e.to_string()) });

                HttpResponse::Ok().content_type("application/json").json(response)
            }
        },
        Err(e) => {
            eprintln!("Error executing query: {:?}", e);

            let response = json!({ "status": "Failed", "message": format!("Failed to execute delete query : {}", e.to_string()) });

            HttpResponse::Ok().content_type("application/json").json(response)
            
        }
    }
}

#[post("/create")]
async fn create_data(data: web::Json<CreateData>) -> impl Responder{
    let session = SESSION.get().expect("Session not initialized");

    let create_query = format!("INSERT INTO todo_app.lists (id, title, description, insertion_time, status) VALUES (uuid(), '{title}', '{description}', {timestamp}, false);", title=data.title, description=data.description, timestamp=data.timestamp);

    match session.query(create_query).await {
        Ok(result) => match result.response_body() {
            Ok(_) => {
                let response = json!({ "status": "Sucess", "message": "Successfully create data" });

                HttpResponse::Ok().content_type("application/json").json(response)
            },
            Err(e) => {
                let response = json!({ "status": "Failed", "message": format!("Failed to process response body : {}", e.to_string()) });

                HttpResponse::Ok().content_type("application/json").json(response)
            }
        },
        Err(e) => {
            eprintln!("Error executing query: {:?}", e);

            let response = json!({ "status": "Failed", "message": format!("Failed to execute create query : {}", e.to_string()) });

            HttpResponse::Ok().content_type("application/json").json(response)
            
        }
    }
}

#[post("/update")]
async fn update_data(data: web::Json<TodoData>) -> impl Responder{
    let session = SESSION.get().expect("Session not initialized");

    let create_query = format!("UPDATE todo_app.lists SET status = {status}, description = '{description}', title = '{title}', insertion_time = {timestamp} WHERE id = {id}", id=data.id, title=data.title, description=data.description, timestamp=data.timestamp, status=data.status);

    match session.query(create_query).await {
        Ok(result) => match result.response_body() {
            Ok(_) => {
                let response = json!({ "status": "Sucess", "message": "Successfully update data" });

                HttpResponse::Ok().content_type("application/json").json(response)
            },
            Err(e) => {
                let response = json!({ "status": "Failed", "message": format!("Failed to process response body : {}", e.to_string()) });

                HttpResponse::Ok().content_type("application/json").json(response)
            }
        },
        Err(e) => {
            eprintln!("Error executing query: {:?}", e);

            let response = json!({ "status": "Failed", "message": format!("Failed to execute update query : {}", e.to_string()) });

            HttpResponse::Ok().content_type("application/json").json(response)
            
        }
    }
}