use lettre::{Message, SmtpTransport, Transport};
use std::env;

pub fn send_email(subject: String, body: String) {
    let from_email = env::var("SMTP_FROM").unwrap();
    let to_email = env::var("SMTP_TO").unwrap();
    let message = Message::builder()
        .from(from_email.parse().unwrap())
        .to(to_email.parse().unwrap())
        .subject(subject)
        .body(body)
        .unwrap();

    let host = env::var("SMTP_HOST").unwrap();
    let port = std::env::var("SMTP_PORT").unwrap().parse::<u16>().unwrap();
    let mailer = SmtpTransport::starttls_relay(&host).unwrap()
        .port(port)
        .build();

    mailer.send(&message).unwrap();
}