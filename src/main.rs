use std::fs::File;
use std::io::{self, Read, Write};

const MAGIC: &[u8; 4] = b"FLOW";
const VERSION: u16 = 1;

fn write_flow(path: &str, records: &[String]) -> io::Result<()> {
    let mut file = File::create(path)?;

    // Write magic bytes: identifies the file type
    file.write_all(MAGIC)?;

    // Write version number
    file.write_all(&VERSION.to_le_bytes())?;

    // Write number of records
    let count = records.len() as u32;
    file.write_all(&count.to_le_bytes())?;

    // Write each record
    for record in records {
        let bytes = record.as_bytes();
        let len = bytes.len() as u32;

        // First write the length of the string
        file.write_all(&len.to_le_bytes())?;

        // Then write the string bytes
        file.write_all(bytes)?;
    }

    Ok(())
}

fn read_flow(path: &str) -> io::Result<Vec<String>> {
    let mut file = File::open(path)?;

    // Read and validate magic bytes
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid .flow file: bad magic bytes",
        ));
    }

    // Read version
    let mut version_bytes = [0u8; 2];
    file.read_exact(&mut version_bytes)?;
    let version = u16::from_le_bytes(version_bytes);

    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unsupported .flow version: {}", version),
        ));
    }

    // Read record count
    let mut count_bytes = [0u8; 4];
    file.read_exact(&mut count_bytes)?;
    let count = u32::from_le_bytes(count_bytes);

    let mut records = Vec::new();

    for _ in 0..count {
        // Read string length
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read string bytes
        let mut buffer = vec![0u8; len];
        file.read_exact(&mut buffer)?;

        // Convert bytes to String
        let record = String::from_utf8(buffer)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 in record"))?;

        records.push(record);
    }

    Ok(records)
}

fn main() -> io::Result<()> {
    let records = vec![
        "apple".to_string(),
        "banana".to_string(),
        "carrot".to_string(),
    ];

    write_flow("data.flow", &records)?;
    println!("Wrote data.flow");

    let loaded = read_flow("data.flow")?;
    println!("Read from data.flow:");

    for record in loaded {
        println!("- {}", record);
    }

    Ok(())
}
