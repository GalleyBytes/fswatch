use base64::{engine::general_purpose, Engine as _};
use inclusterget;
use notify::Event;
use notify::{RecursiveMode, Watcher};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::process::exit;
use std::str;
use std::str::Chars;
use std::sync::mpsc::{self, Sender};
use std::time::Duration;
use std::{env, thread};

#[derive(Debug)]
struct Log {
    pub uuid: String,
    pub task_name: String,
    pub rerun_id: String,
    pub content: String,
}

impl Log {
    fn new(filepath: &str, uuid: &str, task_name: &str, rerun_id: &str) -> std::io::Result<Log> {
        let mut f = File::open(filepath)?;
        let mut buffer = String::new();
        f.read_to_string(&mut buffer)?;
        Ok(Log {
            uuid: uuid.to_string(),
            task_name: task_name.to_string(),
            rerun_id: rerun_id.to_string(),
            content: buffer,
        })
    }
}

#[derive(Deserialize, Debug)]
struct GenericTfoApiResponse {
    status_info: StatusInfo,
    data: Option<Value>,
}

#[derive(Deserialize, Debug)]
struct StatusInfo {
    status_code: u8,
    message: Option<String>,
}

#[derive(Debug)]
struct FileHandler {
    log: Log,
}

impl FileHandler {
    fn new(filepath: &str) -> std::io::Result<FileHandler> {
        let filename = base(filepath);
        let filename_parts: Vec<&str> = filename.split(".").collect();
        if filename_parts.len() != 4 {
            Err(Error::new(ErrorKind::Other, "Not correct number of dots"))
            // Err(Error())
        } else {
            let task_name = filename_parts[0];
            let rerun_id = filename_parts[1];
            let uuid = filename_parts[2];

            let is_rerun_id_an_int = rerun_id.parse::<u32>().is_ok();
            let is_uuid = is_valid_uuid(uuid);

            if !is_rerun_id_an_int {
                Err(Error::new(ErrorKind::Other, "Wrong rerun_id format"))
            } else if !is_uuid {
                Err(Error::new(ErrorKind::Other, "Wrong uuid format"))
            } else {
                let log = Log::new(filepath, uuid, task_name, rerun_id)?;
                Ok(FileHandler { log })
            }
        }
    }

    fn conent(&self) -> &str {
        &self.log.content
    }

    fn uuid(&self) -> &str {
        &self.log.uuid
    }

    fn task_name(&self) -> &str {
        &self.log.task_name
    }

    fn rerun_id(&self) -> &str {
        &self.log.rerun_id
    }
}

#[derive(Debug, Deserialize)]
struct Pod {
    status: PodStatus,
}

#[derive(Debug, Deserialize)]
struct PodStatus {
    #[serde(rename = "containerStatuses")]
    container_statuses: Vec<ContainerStatus>,
}

#[derive(Debug, Deserialize)]
struct ContainerStatus {
    name: String,
    state: ContainerState,
}

#[derive(Debug, Deserialize)]
struct ContainerState {
    terminated: Option<TerminatedState>,
}

#[derive(Debug, Deserialize)]
struct TerminatedState {
    #[serde(rename = "exitCode")]
    exit_code: i32,
}

#[derive(Debug, Deserialize)]
struct Payload {
    generation: String,
    // claims: HashMap<&str, &str>,
}

#[derive(Debug, Clone)]
struct APIClient {
    url: String,
    token: String,
    generation: String,
    in_cluster_generation: String,
    refresh_token_path: String,
    token_path: String,
}

impl APIClient {
    fn new(
        url: String,
        token: String,
        in_cluster_generation: &str,
        refresh_token_path: String,
        token_path: String,
    ) -> APIClient {
        let api_client = APIClient {
            url: url,
            token: token,
            generation: String::new(),
            in_cluster_generation: in_cluster_generation.to_string(),
            refresh_token_path: refresh_token_path,
            token_path: token_path,
        };

        api_client
    }

    /// TODO future
    // fn get_token(&self) {}

    /// TODO future
    // fn check_token_expiration(&self) {}
    fn read_refresh_token(&self) -> String {
        read_file(&self.refresh_token_path).expect("Could not read file")
    }

    fn parse_claims(&mut self) {
        let jwt_items: Vec<&str> = self.token.split(".").collect();

        let payload_raw = jwt_items[1];
        let n = payload_raw.len() % 4;
        let payload_b64encoded_urlsafe = if n > 0 {
            format!("{:=<pad$}!", payload_raw, pad = n)
        } else {
            payload_raw.to_string()
        };

        let decoded_bytes = general_purpose::STANDARD
            .decode(payload_b64encoded_urlsafe)
            .expect("Could not decode token");
        let mut decoded_string = str::from_utf8(&decoded_bytes).expect("Token is not utf8");
        let d = decoded_string.to_string();
        decoded_string = &d;

        let payload: Payload = serde_json::from_str(decoded_string)
            .expect(format!("Couldn't get json from string {decoded_string}").as_str());

        self.generation = payload.generation;
    }

    #[tokio::main]
    async fn upload(
        &mut self,
        file_handler: &FileHandler,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let log_url = format!("{}/api/v1/task", self.url);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "Token",
            reqwest::header::HeaderValue::from_str(self.token.as_str()).unwrap(),
        );

        let mut map = HashMap::new();
        map.insert("content", file_handler.conent());
        map.insert("uuid", file_handler.uuid());
        map.insert("task_name", file_handler.task_name());
        map.insert("rerun_id", file_handler.rerun_id());
        map.insert("generation", &self.in_cluster_generation);

        let client = reqwest::Client::new();
        let response = client
            .post(log_url)
            .headers(headers.clone())
            .json(&map)
            .send()
            .await?;

        if response.status() == StatusCode::UNAUTHORIZED {
            // The token is expired or invalid
            let mut refresh_attempts = 0;
            let max_refresh_attempts = 6;
            loop {
                if refresh_attempts > max_refresh_attempts {
                    break;
                }
                match self.get_new_token().await {
                    Ok(token) => {
                        println!("INFO new token found");
                        self.token = token;
                        return Ok(true);
                    }
                    Err(err) => {
                        println!("ERROR {}", err);
                        refresh_attempts += 1;
                    }
                }
                thread::sleep(Duration::from_secs(15));
            }
            println!("ERROR failed all attempts to get a new token");
            return Ok(false);
        } else {
            let message = response.text().await?;
            if message != "" {
                println!("INFO {}", message);
            }
        }

        Ok(false)
    }

    /// First read token from disk. If unchanged, read the refresh_token from disk and request a new token.
    async fn get_new_token(&self) -> Result<String, Box<dyn std::error::Error>> {
        if self.token.trim() != read_file(&self.token_path)?.trim() {
            return Ok(read_file(&self.token_path)?);
        }

        let client = reqwest::Client::new();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "Token",
            reqwest::header::HeaderValue::from_str(self.token.as_str()).unwrap(),
        );

        let refresh_token = self.read_refresh_token();
        let response = client
            .post(format!("{}/refresh", self.url))
            .headers(headers)
            .json(&json!({"refresh_token": refresh_token}))
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(response.text().await?.into());
        } else {
            let api_response = response.json::<GenericTfoApiResponse>().await?;
            let data = api_response
                .data
                .expect("Refresh token response did not contain new JWT");
            let arr = data
                .as_array()
                .expect("Refresh token repsosne was not properly formatted");
            if arr.len() != 1 {
                return Err("Refresh token response did not contain data".into());
            }
            let new_token = arr
                .get(0)
                .unwrap()
                .as_str()
                .expect("Refresh token data was not properly formatted");
            return Ok(new_token.to_string());
        }
    }
}

fn read_file(filepath: &str) -> std::io::Result<String> {
    let mut f = File::open(filepath)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;
    Ok(buffer)
}

/// Check if is a write event. Ignore events like metadata changes.
fn is_write_event(event: &Event) -> bool {
    if let notify::EventKind::Modify(modify_kind) = event.kind {
        match modify_kind {
            notify::event::ModifyKind::Data(_) => true,
            _ => false,
        }
    } else {
        // false
        event.kind.is_create()
    }
}

/// Extract the filename from a filepath ie the last item after the last '/'
fn base(filepath: &str) -> String {
    let items: Vec<&str> = filepath.split("/").collect();
    items.last().unwrap().to_string()
}

/// Check if a &str is a uuid
fn is_valid_uuid(uuid: &str) -> bool {
    // example: ad7e47a1-15a1-44f8-b4cb-924b02e1cc89
    if uuid.len() != 36 {
        return false;
    } else {
        let uuid_items: Vec<&str> = uuid.split("-").collect();
        if uuid_items.len() != 5 {
            return false;
        } else {
            for (index, &item) in uuid_items.iter().enumerate() {
                // UUID format is 8-4-4-4-12 chars + alphanumeric
                let is_correct_chars = match index {
                    0 => item.chars().count() == 8 && is_all_alphanumeric(item.chars()),
                    1 => item.chars().count() == 4 && is_all_alphanumeric(item.chars()),
                    2 => item.chars().count() == 4 && is_all_alphanumeric(item.chars()),
                    3 => item.chars().count() == 4 && is_all_alphanumeric(item.chars()),
                    4 => item.chars().count() == 12 && is_all_alphanumeric(item.chars()),
                    _ => false,
                };
                if !is_correct_chars {
                    return false;
                }
            }
        }
    }
    true
}

/// Check that all chars in an array of chars are alphanumric
fn is_all_alphanumeric(ch: Chars<'_>) -> bool {
    let b: bool = true;
    for a in ch {
        if !a.is_alphanumeric() {
            return false;
        }
    }
    b
}

/// For every log event, send a POST request of logs
fn post_on_event(event: Event, api_client: &mut APIClient, tx: &Sender<bool>) {
    if !is_write_event(&event) {
        return;
    }
    if event.paths.len() != 1 {
        return;
    }
    let filepath = event.paths[0].to_str().unwrap();

    let _ = tx.send(true);
    post_log(filepath, api_client);
    loop {
        match tx.send(false) {
            Ok(_) => break,
            Err(_) => continue,
        }
    }
}

/// Send a POST request for the filepath specified
fn post_log(filepath: &str, api_client: &mut APIClient) {
    if !filepath.ends_with(".out") {
        return;
    }
    let file_handler_result = FileHandler::new(filepath);
    if file_handler_result.is_err() {
        // println!("File: {}, Error:{:?}",filepath,file_handler_result.unwrap_err());
        return;
    }
    let file_handler = file_handler_result.unwrap();

    println!("INFO Handling uuid: {}", file_handler.uuid());

    match api_client.upload(&file_handler) {
        Ok(rerun) => {
            if rerun {
                println!("INFO Retry sending logs");
                return post_log(filepath, api_client);
            }
        }
        Err(err) => {
            println!("INFO File: {}, Error:{:?}", filepath, err);
            return;
        }
    }
}

/// Setup and start fs notify.
/// For each nitification, send a http POST request with full log data.
fn run_notifier(
    api_client: APIClient,
    logpath: String,
    group: String,
    kind: String,
    namespace: String,
    resource: String,
) -> core::result::Result<(), notify::Error> {
    let (tx, rx) = mpsc::channel();
    let mut inflight = false;
    let mut api_client = api_client.clone();
    println!("INFO Configuring notifier");
    // Convenience method for creating the RecommendedWatcher for the current platform in immediate mode
    let mut watcher = notify::recommended_watcher(move |res| match res {
        Ok(event) => post_on_event(event, &mut api_client, &tx),
        Err(e) => println!("ERROR watch error: {:?}", e),
    })?;

    loop {
        let ready = std::fs::metadata(logpath.as_str());
        match ready {
            Ok(_) => break,
            Err(e) => println!("INFO {}: {}", logpath, e.to_string()),
        }
        thread::sleep(Duration::from_secs(1));
    }

    watcher.watch(Path::new(logpath.as_str()), RecursiveMode::NonRecursive)?;
    println!("INFO Watch started at {}", logpath);
    let mut last_err_code: u8 = 0;
    loop {
        // The purpose of this loop is primarily to keep the notifier alive.
        // The secondary usage is to watch for the termination of container for a given kubernetes pod.
        // Upon termination, the program will exit.
        let resp = inclusterget::get(
            group.clone(),
            kind.clone(),
            namespace.clone(),
            resource.clone(),
        );

        if resp.is_err() {
            let err = resp.unwrap_err();
            if last_err_code != err.1 {
                println!("ERROR {}", err.0);
                last_err_code = err.1;
            }
            thread::sleep(Duration::from_secs(5));
            continue;
        }
        let mut response = resp.unwrap();
        let parsed_pod: Result<Pod, serde_json::Error> = serde_json::from_str(response.as_str());
        if parsed_pod.is_err() {
            // arbitrary err_code is 9
            if last_err_code != 9 {
                response.pop();
                println!(
                    "ERROR unable to parse reponse: {}: {}",
                    response,
                    parsed_pod.unwrap_err().to_string()
                );
                last_err_code = 9;
            }
            thread::sleep(Duration::from_secs(5));
            continue;
        };
        let pod = parsed_pod.unwrap_or(Pod {
            status: PodStatus {
                container_statuses: vec![],
            },
        });
        for item in pod.status.container_statuses {
            if item.name == String::from("task") {
                if item.state.terminated.is_some() {
                    println!("INFO Main task complete. Checking if safe to exit...");
                    loop {
                        // Consume all the messages updating inflight
                        match rx.try_recv() {
                            Ok(b) => inflight = b,
                            Err(_) => break,
                        };
                    }
                    println!("INFO API request inflight: {}", inflight);
                    loop {
                        if inflight {
                            // block until
                            inflight = rx.recv().unwrap();
                            println!("INFO API request inflight={}", inflight);
                        } else {
                            break;
                        }
                    }
                    println!("INFO Exiting");
                    exit(item.state.terminated.unwrap().exit_code);
                }
            }
        }

        // clear the message queue periodically
        loop {
            match rx.try_recv() {
                Ok(b) => inflight = b,
                Err(_) => break,
            };
        }
        thread::sleep(Duration::from_secs(5));
    }
}

fn main() {
    let group = String::from("v1");
    let kind = String::from("pods");
    let namespace = env::var("TFO_NAMESPACE").unwrap_or(String::from("default"));
    let resource = env::var("HOSTNAME").unwrap_or(String::from("-"));

    let url = env::var("TFO_API_URL").expect("$TFO_API_URL is not set");
    let token = env::var("TFO_API_LOG_TOKEN").expect("$TFO_API_LOG_TOKEN is not set");
    let token_path =
        env::var("TFO_API_LOG_TOKEN_PATH").unwrap_or(String::from("/jwt/TFO_API_LOG_TOKEN"));
    let refresh_token_path =
        env::var("REFRESH_TOKEN_PATH").unwrap_or(String::from("/jwt/REFRESH_TOKEN"));

    let generation = env::var("TFO_GENERATION").expect("$TFO_GENERATION is not set");
    let api_client = APIClient::new(
        url,
        token,
        generation.as_str(),
        refresh_token_path,
        token_path,
    );

    let tfo_root_path = env::var("TFO_ROOT_PATH").expect("$TFO_ROOT_PATH is not set");
    let logpath = format!("{tfo_root_path}/generations/{}", generation);

    run_notifier(api_client, logpath, group, kind, namespace, resource)
        .expect("Failed to start notifier");
}
