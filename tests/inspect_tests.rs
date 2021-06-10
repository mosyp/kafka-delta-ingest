use deltalake::action::Action;
use std::io::{BufRead, BufReader, Cursor};
use std::process::Command;
use uuid::Uuid;

#[tokio::test]
async fn inspect() {
    std::env::set_var("AWS_ENDPOINT_URL", "http://0.0.0.0:4566");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");

    let path = "s3://tests/emails-eb4ade0f-c77f-4313-bcd3-4dc737ffb5c4";
    let table = deltalake::open_table(path).await.unwrap();

    println!("Table: {}", path);
    for (k, v) in table.get_app_transaction_version().iter() {
        println!("{}: {}", k, v);
    }

    let backend = deltalake::get_backend_for_uri(path).unwrap();

    for version in 1..=table.version {
        let log_path = format!("{}/_delta_log/{:020}.json", path, version);
        let bytes = backend.get_obj(&log_path).await.unwrap();
        let reader = BufReader::new(Cursor::new(bytes));

        println!("Version {}:", version);

        for line in reader.lines() {
            let action: Action = serde_json::from_str(line.unwrap().as_str()).unwrap();
            match action {
                Action::add(a) => {
                    println!("File: {}", &a.path);
                    let uuid = format!("{}.parquet", Uuid::new_v4());
                    let full_path = format!("{}/{}", &path, &a.path);
                    let _ = Command::new("aws")
                        .args(&[
                            "s3",
                            "cp",
                            &full_path,
                            &uuid,
                            "--endpoint-url=http://0.0.0.0:4566",
                        ])
                        .output()
                        .unwrap();
                    let out = Command::new("parquet-tools")
                        .args(&["cat", "--json", &uuid])
                        .output()
                        .unwrap();
                    println!("{}", std::str::from_utf8(&out.stdout).unwrap());
                }
                _ => (),
            }
        }
        println!();
    }
}
