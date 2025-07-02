pub fn parse_signature_script(signature_script: &[u8]) -> Vec<(String, String)> {
    let mut result: Vec<(String, String)> = Vec::new();

    let mut offset = 0;

    while offset < signature_script.len() {
        let opcode = signature_script[offset];
        offset += 1;

        if (0x01..=0x4b).contains(&opcode) {
            let data_length = opcode as usize;

            // Check if we have enough bytes remaining for the data
            if offset + data_length > signature_script.len() {
                break; // Not enough data, stop parsing
            }

            let data = &signature_script[offset..offset + data_length];
            offset += data_length;

            if is_human_readable(data) {
                result.push((
                    String::from("OP_PUSH"),
                    String::from_utf8(data.to_vec()).unwrap(),
                ));
            } else {
                result.push((String::from("OP_PUSH"), hex::encode(data)));
            }
        } else if opcode == 0x4c {
            // Check if we have at least one byte for the length
            if offset >= signature_script.len() {
                break; // Not enough data for length byte
            }

            let data_length = signature_script[offset] as usize;
            offset += 1;

            // Check if we have enough bytes remaining for the data
            if offset + data_length > signature_script.len() {
                break; // Not enough data, stop parsing
            }

            let data = &signature_script[offset..offset + data_length];
            offset += data_length;

            if is_human_readable(data) {
                result.push((
                    String::from("OP_PUSHDATA1"),
                    String::from_utf8(data.to_vec()).unwrap(),
                ));
            } else {
                let nested_result = parse_signature_script(data);
                result.extend(nested_result);
            }
        } else if opcode == 0x00 {
            result.push((String::from("OP_0"), String::from("")));
        } else if opcode == 0x51 {
            result.push((String::from("OP_1"), String::from("")));
        } else if opcode == 0x63 {
            result.push((String::from("OP_IF"), String::from("")));
        } else if opcode == 0x68 {
            result.push((String::from("OP_ENDIF"), String::from("")));
        } else if opcode == 0xac {
            result.push((String::from("OP_CHECKSIG"), String::from("")));
        } else {
            result.push((String::from("OP_UNKNOWN"), String::from("")));
        }
    }

    result
}

fn is_human_readable(data: &[u8]) -> bool {
    data.iter().all(|&b|  (0x02..=0x7e).contains(&b))
}
