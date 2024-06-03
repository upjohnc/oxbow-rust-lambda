default:
    @just --list

# Terraform

tf-init:
    cd deployment && terraform init

tf-plan:
    cd deployment && terraform plan

tf-apply:
    cd deployment && terraform apply -auto-approve

tf-destroy:
    cd deployment && terraform destroy

# Build rust code

bin := "oxbow-lambda"
cargo-lambda:
    cargo lambda build --release --arm64 --output-format zip --bin {{ bin }}

run-local-file:
    cargo run --manifest-path local_add_file/Cargo.toml
