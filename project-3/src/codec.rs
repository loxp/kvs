use crate::Result;

/// Message type for encoding and decoding
pub type Message = Vec<String>;

/// Decoding a line into a message
pub fn decode(line: String) -> Result<Message> {
    let tokens = line
        .split(" ")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    Ok(tokens)
}

/// Encoding a message into a String line
pub fn encode(req: Message) -> Result<String> {
    let mut ret = String::new();
    for s in req.iter() {
        ret.push_str(s);
        ret.push(' ');
    }
    ret.push('\n');
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_from_string() {
        struct Testcase {
            input: &'static str,
            expect: &'static [&'static str],
        }

        let mut testcases = Vec::new();
        testcases.push(Testcase {
            input: "get key",
            expect: &["get", "key"],
        });
        testcases.push(Testcase {
            input: "get  key ",
            expect: &["get", "key"],
        });
        testcases.push(Testcase {
            input: "get  key11 ",
            expect: &["get", "key11"],
        });
        testcases.push(Testcase {
            input: "set  key11 hello",
            expect: &["set", "key11", "hello"],
        });

        for i in 0..testcases.len() {
            let testcase = testcases.get(i).unwrap();
            let cmd = decode(testcase.input.to_string()).unwrap();
            for j in 0..testcase.expect.len() {
                let a = testcase.expect[j].to_string();
                let b = cmd.get(j).unwrap().to_string();
                assert_eq!(a, b);
            }
        }
    }
}
