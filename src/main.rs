use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::process::exit;
use std::str;
use std::str::Chars;
use std::time::Duration;
use std::{env, thread};

use base64::{engine::general_purpose, Engine as _};
use inclusterget;
use notify::Event;
use notify::{RecursiveMode, Watcher};
use serde::Deserialize;

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

#[derive(Debug)]
struct APIClient {
    url: String,
    token: String,
    generation: String,
    in_cluster_generation: String,
}

impl APIClient {
    fn new(url: String, token: String, in_cluster_generation: &str) -> APIClient {
        let api_client = APIClient {
            url: url,
            token: token,
            generation: String::new(),
            in_cluster_generation: in_cluster_generation.to_string(),
        };

        api_client
    }

    /// TODO future
    // fn get_token(&self) {}

    /// TODO future
    // fn check_token_expiration(&self) {}

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
    async fn upload(&self, file_handler: FileHandler) -> Result<(), Box<dyn std::error::Error>> {
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
        let res = client
            .post(log_url)
            .headers(headers)
            .json(&map)
            .send()
            .await?
            .text()
            .await?;
        if res != "" {
            println!("INFO {}", res);
        }
        Ok(())
    }
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
fn post_on_event(event: Event, api_client: &APIClient) {
    if !is_write_event(&event) {
        return;
    }
    if event.paths.len() != 1 {
        return;
    }
    let filepath = event.paths[0].to_str().unwrap();
    post_log(filepath, api_client);
}

/// Send a POST request for the filepath specified
fn post_log(filepath: &str, api_client: &APIClient) {
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

    let upload_result = api_client.upload(file_handler);
    if upload_result.is_err() {
        println!(
            "INFO File: {}, Error:{:?}",
            filepath,
            upload_result.unwrap_err()
        );
        return;
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
    println!("INFO Configuring notifier");
    // Convenience method for creating the RecommendedWatcher for the current platform in immediate mode
    let mut watcher = notify::recommended_watcher(move |res| match res {
        Ok(event) => post_on_event(event, &api_client),
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
                    println!("INFO Task terminated. Shutting down watch service");
                    // Allow time to complete inflight api calls
                    thread::sleep(Duration::from_secs(3));
                    exit(item.state.terminated.unwrap().exit_code);
                }
            }
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
    let generation = env::var("TFO_GENERATION").expect("$TFO_GENERATION is not set");
    let api_client = APIClient::new(url, token, generation.as_str());

    let tfo_root_path = env::var("TFO_ROOT_PATH").expect("$TFO_ROOT_PATH is not set");
    let logpath = format!("{tfo_root_path}/generations/{}", generation);

    run_notifier(api_client, logpath, group, kind, namespace, resource)
        .expect("Failed to start notifier");
}
