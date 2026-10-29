use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pgwire::api::Type;
use pgwire::api::copy::CopyDecoder;
use pgwire::api::results::{CopyCsvOptions, CopyEncoder, CopyTextOptions, FieldFormat, FieldInfo};
use pgwire::messages::copy::CopyData;
use pgwire::messages::data::DataRow;

type Values = Vec<Vec<Option<Vec<u8>>>>;

fn copy_data(data: &[u8]) -> CopyData {
    CopyData::new(Bytes::copy_from_slice(data))
}

fn unpack_row(row: DataRow) -> Vec<Option<Vec<u8>>> {
    assert!(row.field_count >= 0);
    let mut data = row.data.freeze();
    let mut values = Vec::with_capacity(row.field_count as usize);

    for _ in 0..row.field_count {
        assert!(data.remaining() >= 4, "field length is missing");
        let len = data.get_i32();
        match len {
            -1 => values.push(None),
            value if value >= 0 => {
                let len = value as usize;
                assert!(data.remaining() >= len, "field value is truncated");
                values.push(Some(data.copy_to_bytes(len).to_vec()));
            }
            value => panic!("invalid DataRow field length {value}"),
        }
    }

    assert_eq!(data.remaining(), 0, "DataRow contains trailing data");
    values
}

fn decode_chunks(mut decoder: CopyDecoder, input: &[u8], chunk_ends: &[usize]) -> Values {
    let mut rows = Vec::new();
    let mut start = 0;

    for &end in chunk_ends {
        assert!(end >= start && end <= input.len());
        rows.extend(decoder.push(copy_data(&input[start..end])).unwrap());
        start = end;
    }

    if start < input.len() {
        rows.extend(decoder.push(copy_data(&input[start..])).unwrap());
    }
    rows.extend(decoder.finish().unwrap());
    rows.into_iter().map(unpack_row).collect()
}

fn assert_rejected_at_every_split<F>(input: &[u8], make: F)
where
    F: Fn() -> CopyDecoder,
{
    for split in 0..=input.len() {
        let mut decoder = make();
        let first = decoder.push(copy_data(&input[..split]));
        if split == input.len() {
            assert!(first.is_err(), "accepted invalid prefix {input:?}");
        } else {
            assert!(
                matches!(first, Ok(rows) if rows.is_empty()),
                "rejected before decisive byte {split} of {input:?}"
            );
            assert!(
                decoder.push(copy_data(&input[split..])).is_err(),
                "accepted invalid prefix {input:?} split at {split}"
            );
        }
        assert!(decoder.push(copy_data(b"")).is_err());
        assert!(decoder.finish().is_err());
    }
}

fn assert_all_two_way_splits<F>(input: &[u8], expected: &Values, make: F)
where
    F: Fn() -> CopyDecoder,
{
    assert_eq!(decode_chunks(make(), input, &[]), *expected);
    for split in 0..=input.len() {
        assert_eq!(
            decode_chunks(make(), input, &[split]),
            *expected,
            "different result with split at byte {split}"
        );
    }
}

fn assert_bytewise<F>(input: &[u8], expected: &Values, make: F)
where
    F: Fn() -> CopyDecoder,
{
    let ends: Vec<_> = (0..input.len()).collect();
    assert_eq!(decode_chunks(make(), input, &ends), *expected);
}

fn binary_header(flags: i32, extension: &[u8]) -> BytesMut {
    let mut data = BytesMut::new();
    data.extend_from_slice(b"PGCOPY\n\xFF\r\n\0");
    data.put_i32(flags);
    data.put_i32(extension.len() as i32);
    data.extend_from_slice(extension);
    data
}

fn put_binary_row(data: &mut BytesMut, fields: &[Option<&[u8]>]) {
    data.put_i16(fields.len() as i16);
    for field in fields {
        match field {
            Some(value) => {
                data.put_i32(value.len() as i32);
                data.extend_from_slice(value);
            }
            None => data.put_i32(-1),
        }
    }
}

#[test]
fn constructors_validate_counts_and_format_characters() {
    let _root_decoder: pgwire::api::CopyDecoder =
        pgwire::api::copy::CopyDecoder::new_binary(1).unwrap();

    for columns in [0, i16::MAX as usize + 1, 65_537] {
        assert!(CopyDecoder::new_binary(columns).is_err());
        assert!(CopyDecoder::new_text(columns, CopyTextOptions::default()).is_err());
        assert!(CopyDecoder::new_csv(columns, CopyCsvOptions::default()).is_err());
    }
    for columns in [1, i16::MAX as usize] {
        assert!(CopyDecoder::new_binary(columns).is_ok());
        assert!(CopyDecoder::new_text(columns, CopyTextOptions::default()).is_ok());
        assert!(CopyDecoder::new_csv(columns, CopyCsvOptions::default()).is_ok());
    }

    for delimiter in ["", "::", "\u{00e9}", "\n", "\r", "\\", "."] {
        assert!(
            CopyDecoder::new_text(
                1,
                CopyTextOptions {
                    delimiter: delimiter.into(),
                    ..Default::default()
                }
            )
            .is_err(),
            "text delimiter {delimiter:?} should be rejected"
        );
    }
    for delimiter in (b'a'..=b'z').chain(b'0'..=b'9') {
        let delimiter = char::from(delimiter).to_string();
        assert!(
            CopyDecoder::new_text(
                1,
                CopyTextOptions {
                    delimiter: delimiter.clone().into(),
                    ..Default::default()
                }
            )
            .is_err(),
            "text delimiter {delimiter:?} should be rejected"
        );
    }

    assert!(
        CopyDecoder::new_text(
            1,
            CopyTextOptions {
                delimiter: "|".into(),
                null_string: "NULL".into(),
            }
        )
        .is_ok()
    );

    let uppercase = CopyTextOptions {
        delimiter: "A".into(),
        null_string: "NULL".into(),
    };
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_text(2, uppercase).unwrap(),
            b"leftAright\n",
            &[],
        ),
        vec![vec![Some(b"left".to_vec()), Some(b"right".to_vec())]]
    );

    for delimiter in ["a", "0", ".", "\\"] {
        let options = CopyCsvOptions {
            delimiter: delimiter.into(),
            ..Default::default()
        };
        let input = format!("left{delimiter}right\n");
        assert_eq!(
            decode_chunks(
                CopyDecoder::new_csv(2, options).unwrap(),
                input.as_bytes(),
                &[],
            ),
            vec![vec![Some(b"left".to_vec()), Some(b"right".to_vec())]],
            "CSV delimiter {delimiter:?} should be accepted"
        );
    }

    let delimiter_equals_escape = CopyCsvOptions {
        delimiter: "|".into(),
        quote: "'".into(),
        escape: "|".into(),
        null_string: "NULL".into(),
        ..Default::default()
    };
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_csv(2, delimiter_equals_escape).unwrap(),
            b"left|right\n",
            &[],
        ),
        vec![vec![Some(b"left".to_vec()), Some(b"right".to_vec())]]
    );

    for delimiter in ['|', ';', '\t'] {
        for null_string in [
            format!("{delimiter}edge"),
            format!("mid{delimiter}dle"),
            format!("edge{delimiter}"),
        ] {
            assert!(
                CopyDecoder::new_text(
                    1,
                    CopyTextOptions {
                        delimiter: delimiter.to_string().into(),
                        null_string: null_string.into(),
                    }
                )
                .is_err()
            );
        }
    }
    for forbidden in ['\n', '\r'] {
        for null_string in [
            format!("{forbidden}edge"),
            format!("mid{forbidden}dle"),
            format!("edge{forbidden}"),
        ] {
            assert!(
                CopyDecoder::new_text(
                    1,
                    CopyTextOptions {
                        delimiter: "|".into(),
                        null_string: null_string.into(),
                    }
                )
                .is_err()
            );
        }
    }

    let invalid_csv = [
        CopyCsvOptions {
            delimiter: "".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            delimiter: "::".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            delimiter: "\u{00e9}".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            delimiter: "\n".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            delimiter: "\r".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            quote: "".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            quote: "::".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            quote: "\u{00e9}".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            escape: "".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            escape: "::".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            escape: "\u{00e9}".into(),
            ..Default::default()
        },
        CopyCsvOptions {
            delimiter: "\"".into(),
            quote: "\"".into(),
            ..Default::default()
        },
    ];
    for options in invalid_csv {
        assert!(CopyDecoder::new_csv(2, options).is_err());
    }

    for (delimiter, quote) in [(',', '"'), (';', '\''), ('|', '~')] {
        for forbidden in [delimiter, quote] {
            for null_string in [
                format!("{forbidden}edge"),
                format!("mid{forbidden}dle"),
                format!("edge{forbidden}"),
            ] {
                assert!(
                    CopyDecoder::new_csv(
                        2,
                        CopyCsvOptions {
                            delimiter: delimiter.to_string().into(),
                            quote: quote.to_string().into(),
                            null_string: null_string.into(),
                            ..Default::default()
                        }
                    )
                    .is_err()
                );
            }
        }
    }
    for forbidden in ['\n', '\r'] {
        for null_string in [
            format!("{forbidden}edge"),
            format!("mid{forbidden}dle"),
            format!("edge{forbidden}"),
        ] {
            assert!(
                CopyDecoder::new_csv(
                    2,
                    CopyCsvOptions {
                        null_string: null_string.into(),
                        ..Default::default()
                    }
                )
                .is_err()
            );
        }
    }

    assert!(
        CopyDecoder::new_csv(
            2,
            CopyCsvOptions {
                delimiter: "|".into(),
                quote: "'".into(),
                escape: "\\".into(),
                null_string: "NULL".into(),
                force_quote: vec![usize::MAX, usize::MAX, 42],
            }
        )
        .is_ok()
    );
}

#[test]
fn empty_chunks_are_noops_and_successful_finish_is_terminal() {
    let mut decoder = CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap();
    assert!(decoder.push(copy_data(b"")).unwrap().is_empty());
    assert!(decoder.push(copy_data(b"")).unwrap().is_empty());
    assert!(decoder.finish().unwrap().is_empty());
    assert!(decoder.push(copy_data(b"later\n")).is_err());
    assert!(decoder.finish().is_err());

    let mut csv = CopyDecoder::new_csv(1, CopyCsvOptions::default()).unwrap();
    assert!(csv.push(copy_data(b"")).unwrap().is_empty());
    assert!(csv.finish().unwrap().is_empty());
    assert!(csv.push(copy_data(b"later\n")).is_err());
    assert!(csv.finish().is_err());

    let mut binary = CopyDecoder::new_binary(1).unwrap();
    assert!(binary.push(copy_data(b"")).unwrap().is_empty());
    let mut empty_stream = binary_header(0, b"");
    empty_stream.put_i16(-1);
    assert!(
        binary
            .push(CopyData::new(empty_stream.freeze()))
            .unwrap()
            .is_empty()
    );
    assert!(binary.finish().unwrap().is_empty());

    let mut text = CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap();
    assert_eq!(
        text.push(copy_data(b"\n"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        vec![vec![Some(vec![])]]
    );
    assert!(text.finish().unwrap().is_empty());

    let mut csv = CopyDecoder::new_csv(1, CopyCsvOptions::default()).unwrap();
    assert_eq!(
        csv.push(copy_data(b"\n"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        vec![vec![None]]
    );
    assert!(csv.finish().unwrap().is_empty());

    let mut text = CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap();
    assert!(text.push(copy_data(b"last")).unwrap().is_empty());
    assert_eq!(text.finish().unwrap().len(), 1);
    assert!(text.push(copy_data(b"later\n")).is_err());
    assert!(text.finish().is_err());

    let mut csv = CopyDecoder::new_csv(1, CopyCsvOptions::default()).unwrap();
    assert!(csv.push(copy_data(b"last")).unwrap().is_empty());
    assert_eq!(csv.finish().unwrap().len(), 1);
    assert!(csv.push(copy_data(b"later\n")).is_err());
    assert!(csv.finish().is_err());

    let mut text = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert!(text.push(copy_data(b"value\t")).unwrap().is_empty());
    assert_eq!(
        text.finish()
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        vec![vec![Some(b"value".to_vec()), Some(vec![])]]
    );

    let custom_null = CopyCsvOptions {
        null_string: "NULL".into(),
        ..Default::default()
    };
    let mut csv = CopyDecoder::new_csv(2, custom_null).unwrap();
    assert!(csv.push(copy_data(b"value,")).unwrap().is_empty());
    assert_eq!(
        csv.finish()
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        vec![vec![Some(b"value".to_vec()), Some(vec![])]]
    );
}

#[test]
fn text_and_csv_emit_completed_rows_from_push() {
    let expected_first = vec![vec![Some(b"a".to_vec()), Some(b"b".to_vec())]];
    let expected_second = vec![vec![Some(b"c".to_vec()), Some(b"d".to_vec())]];

    let mut text = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert_eq!(
        text.push(copy_data(b"a\tb\nc"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected_first
    );
    assert!(text.push(copy_data(b"")).unwrap().is_empty());
    assert_eq!(
        text.push(copy_data(b"\td\n"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected_second
    );
    assert!(text.finish().unwrap().is_empty());

    let mut csv = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert_eq!(
        csv.push(copy_data(b"a,b\nc"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected_first
    );
    assert!(csv.push(copy_data(b"")).unwrap().is_empty());
    assert_eq!(
        csv.push(copy_data(b",d\n"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected_second
    );
    assert!(csv.finish().unwrap().is_empty());

    let expected_both = vec![
        vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
        vec![Some(b"c".to_vec()), Some(b"d".to_vec())],
    ];
    let mut text = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert_eq!(
        text.push(copy_data(b"a\tb\nc\td\n"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected_both
    );
    assert!(text.finish().unwrap().is_empty());

    let mut csv = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert_eq!(
        csv.push(copy_data(b"a,b\nc,d\n"))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected_both
    );
    assert!(csv.finish().unwrap().is_empty());

    let mut text_256 = vec![b'\t'; 255];
    text_256.push(b'\n');
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_text(256, CopyTextOptions::default()).unwrap(),
            &text_256,
            &[1, 127, 255],
        ),
        vec![vec![Some(Vec::new()); 256]]
    );

    let mut csv_256 = vec![b','; 255];
    csv_256.push(b'\n');
    let custom_null = CopyCsvOptions {
        null_string: "NULL".into(),
        ..Default::default()
    };
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_csv(256, custom_null).unwrap(),
            &csv_256,
            &[1, 127, 255],
        ),
        vec![vec![Some(Vec::new()); 256]]
    );
}

#[test]
fn failed_finish_terminalizes_every_decoder_format() {
    let mut text = CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap();
    assert!(text.push(copy_data(b"value\\")).unwrap().is_empty());

    let mut csv = CopyDecoder::new_csv(1, CopyCsvOptions::default()).unwrap();
    assert!(csv.push(copy_data(b"\"value")).unwrap().is_empty());

    let mut binary = CopyDecoder::new_binary(1).unwrap();
    assert!(binary.push(copy_data(b"PG")).unwrap().is_empty());

    for (mut decoder, continuation) in [
        (text, b"n\n".as_slice()),
        (csv, b"\"\n".as_slice()),
        (binary, b"COPY\n\xFF\r\n\0".as_slice()),
    ] {
        assert!(decoder.finish().is_err());
        assert!(decoder.push(copy_data(continuation)).is_err());
        assert!(decoder.finish().is_err());
    }
}

#[test]
fn malformed_input_is_rejected_as_soon_as_determinable() {
    assert_rejected_at_every_split(b"a\t", || {
        CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap()
    });
    assert_rejected_at_every_split(b"a,", || {
        CopyDecoder::new_csv(1, CopyCsvOptions::default()).unwrap()
    });

    let signature = b"PGCOPY\n\xFF\r\n\0";
    for index in 0..signature.len() {
        let mut invalid = signature[..=index].to_vec();
        invalid[index] ^= 1;
        assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
    }

    for bit in 16..32 {
        let flag_bytes = (1u32 << bit).to_be_bytes();
        let decisive_len = flag_bytes.iter().position(|byte| *byte != 0).unwrap() + 1;
        let mut invalid = signature.to_vec();
        invalid.extend_from_slice(&flag_bytes[..decisive_len]);
        assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
    }

    for sign_byte in 0x80u8..=0xFF {
        let mut negative_extension = signature.to_vec();
        negative_extension.extend_from_slice(&[0, 0, 0, 0, sign_byte]);
        assert_rejected_at_every_split(&negative_extension, || CopyDecoder::new_binary(1).unwrap());
    }

    let mut positive_extension_prefix = signature.to_vec();
    positive_extension_prefix.extend_from_slice(&[0, 0, 0, 0, 0x7F]);
    let mut binary = CopyDecoder::new_binary(1).unwrap();
    assert!(
        binary
            .push(copy_data(&positive_extension_prefix))
            .unwrap()
            .is_empty()
    );

    let header = binary_header(0, b"");
    for first_byte in 0x01u8..=0xFE {
        let mut invalid = header.to_vec();
        invalid.push(first_byte);
        assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
    }
    for second_byte in 0x00u8..=0xFF {
        if second_byte != 1 {
            let mut invalid = header.to_vec();
            invalid.extend_from_slice(&[0, second_byte]);
            assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
        }
    }
    for second_byte in 0x00u8..=0xFE {
        let mut invalid = header.to_vec();
        invalid.extend_from_slice(&[0xFF, second_byte]);
        assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
    }

    let mut allowed_tuple_prefix = header.to_vec();
    allowed_tuple_prefix.push(0x01);
    let mut binary = CopyDecoder::new_binary(256).unwrap();
    assert!(
        binary
            .push(copy_data(&allowed_tuple_prefix))
            .unwrap()
            .is_empty()
    );

    for first_byte in 0x80u8..=0xFE {
        let mut invalid = header.to_vec();
        invalid.extend_from_slice(&1i16.to_be_bytes());
        invalid.push(first_byte);
        assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
    }
    for leading_ff in 1..=3 {
        for first_non_ff in 0x00u8..=0xFE {
            let mut invalid = header.to_vec();
            invalid.extend_from_slice(&1i16.to_be_bytes());
            invalid.extend(std::iter::repeat_n(0xFF, leading_ff));
            invalid.push(first_non_ff);
            assert_rejected_at_every_split(&invalid, || CopyDecoder::new_binary(1).unwrap());
        }
    }

    let mut text = CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap();
    assert_eq!(text.push(copy_data(b"a\n")).unwrap().len(), 1);
    assert!(text.push(copy_data(b"b\r")).is_err());

    let mut csv = CopyDecoder::new_csv(1, CopyCsvOptions::default()).unwrap();
    assert_eq!(csv.push(copy_data(b"a\n")).unwrap().len(), 1);
    assert!(csv.push(copy_data(b"b\r")).is_err());
}

#[test]
fn text_rejects_under_width_first_record_at_chunk_final_cr() {
    assert_rejected_at_every_split(b"only-one\r", || {
        CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap()
    });
}

#[test]
fn csv_rejects_under_width_first_record_at_chunk_final_cr() {
    assert_rejected_at_every_split(b"only-one\r", || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });
}

#[test]
fn text_decoding_is_chunk_boundary_invariant() {
    let input = b"one\t\\N\tthree\nline\\nvalue\t\\\\N\t\\x41\\101\nlast\tvalue\tend";
    let expected = vec![
        vec![Some(b"one".to_vec()), None, Some(b"three".to_vec())],
        vec![
            Some(b"line\nvalue".to_vec()),
            Some(b"\\N".to_vec()),
            Some(b"AA".to_vec()),
        ],
        vec![
            Some(b"last".to_vec()),
            Some(b"value".to_vec()),
            Some(b"end".to_vec()),
        ],
    ];

    assert_all_two_way_splits(input, &expected, || {
        CopyDecoder::new_text(3, CopyTextOptions::default()).unwrap()
    });
    assert_bytewise(input, &expected, || {
        CopyDecoder::new_text(3, CopyTextOptions::default()).unwrap()
    });
}

#[test]
fn text_decodes_all_escape_families_and_escaped_separators() {
    let options = CopyTextOptions {
        delimiter: "|".into(),
        null_string: "NULL".into(),
    };
    let input = b"a\\|b|\\b\\f\\n\\r\\t\\v\\\\|\\1\\12\\123\\1234\\x414\\xAf|\\q\nleft\\\nright|NULL|\\x4\\x41\n";
    let expected = [
        vec![
            Some(b"a|b".to_vec()),
            Some(vec![8, 12, 10, 13, 9, 11, b'\\']),
            Some(vec![1, 10, 0o123, 0o123, b'4', b'A', b'4', 0xAF]),
            Some(b"q".to_vec()),
        ],
        vec![Some(b"left\nright".to_vec()), None, Some(b"\x04A".to_vec())],
    ];

    // The first record has four fields and the second has three, so exercise
    // each record with its own expected schema.
    let first_end = input.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    assert_all_two_way_splits(&input[..first_end], &expected[..1].to_vec(), || {
        CopyDecoder::new_text(4, options.clone()).unwrap()
    });
    assert_all_two_way_splits(&input[first_end..], &expected[1..].to_vec(), || {
        CopyDecoder::new_text(3, options.clone()).unwrap()
    });

    for (input, expected_byte) in [
        (b"x\\1".as_slice(), 0o1),
        (b"x\\12".as_slice(), 0o12),
        (b"x\\xA".as_slice(), 0x0A),
    ] {
        let expected = vec![vec![Some(vec![b'x', expected_byte])]];
        assert_all_two_way_splits(input, &expected, || {
            CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap()
        });
    }

    for (input, expected) in [
        (b"\\x\n".as_slice(), b"x".as_slice()),
        (b"\\xq\n".as_slice(), b"xq".as_slice()),
        (b"x\\x".as_slice(), b"xx".as_slice()),
        (b"\\400\\777\n".as_slice(), [0x00, 0xFF].as_slice()),
    ] {
        let expected = vec![vec![Some(expected.to_vec())]];
        assert_all_two_way_splits(input, &expected, || {
            CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap()
        });
    }

    let zero_digit_hex_at_delimiter = vec![vec![Some(b"x".to_vec()), Some(b"value".to_vec())]];
    assert_all_two_way_splits(b"\\x|value\n", &zero_digit_hex_at_delimiter, || {
        CopyDecoder::new_text(2, options.clone()).unwrap()
    });

    let raw_quotes = b"\"left\"\tright\n";
    let raw_quotes_expected = vec![vec![Some(b"\"left\"".to_vec()), Some(b"right".to_vec())]];
    assert_all_two_way_splits(raw_quotes, &raw_quotes_expected, || {
        CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap()
    });
}

#[test]
fn text_hex_escape_consumes_uppercase_hex_delimiters() {
    for delimiter in b'A'..=b'F' {
        let options = CopyTextOptions {
            delimiter: char::from(delimiter).to_string().into(),
            ..Default::default()
        };
        let input = [b'\\', b'x', delimiter];
        let expected = vec![vec![Some(vec![delimiter - b'A' + 10])]];

        assert_all_two_way_splits(&input, &expected, || {
            CopyDecoder::new_text(1, options.clone()).unwrap()
        });
        assert_bytewise(&input, &expected, || {
            CopyDecoder::new_text(1, options.clone()).unwrap()
        });
    }
}

#[test]
fn text_two_digit_hex_escape_precedes_its_field_delimiter() {
    let options = CopyTextOptions {
        delimiter: "F".into(),
        ..Default::default()
    };
    let input = b"\\xAFFtail\n";
    let expected = vec![vec![Some(vec![0xAF]), Some(b"tail".to_vec())]];

    assert_all_two_way_splits(input, &expected, || {
        CopyDecoder::new_text(2, options.clone()).unwrap()
    });
    assert_bytewise(input, &expected, || {
        CopyDecoder::new_text(2, options.clone()).unwrap()
    });
}

#[test]
fn text_matches_null_before_unescaping_and_does_not_treat_backslash_dot_as_eof() {
    let options = CopyTextOptions {
        delimiter: "|".into(),
        null_string: "\\N".into(),
    };
    let rows = decode_chunks(
        CopyDecoder::new_text(2, options).unwrap(),
        b"\\N|\\\\N\n\\.|value\n",
        &[1, 4, 7, 9],
    );
    assert_eq!(
        rows,
        vec![
            vec![None, Some(b"\\N".to_vec())],
            vec![Some(b".".to_vec()), Some(b"value".to_vec())],
        ]
    );

    let trailing_slash_options = CopyTextOptions {
        delimiter: "|".into(),
        null_string: "\\".into(),
    };
    let null_row = vec![vec![None]];
    for input in [
        b"\\".as_slice(),
        b"\\\n".as_slice(),
        b"\\\r".as_slice(),
        b"\\\r\n".as_slice(),
    ] {
        assert_all_two_way_splits(input, &null_row, || {
            CopyDecoder::new_text(1, trailing_slash_options.clone()).unwrap()
        });
    }

    let followed_by_delimiter = vec![vec![None, Some(b"value".to_vec())]];
    assert_all_two_way_splits(b"\\|value\n", &followed_by_delimiter, || {
        CopyDecoder::new_text(2, trailing_slash_options.clone()).unwrap()
    });

    let multi_byte_options = CopyTextOptions {
        delimiter: "|".into(),
        null_string: "abc\\".into(),
    };
    assert_all_two_way_splits(b"abc\\", &null_row, || {
        CopyDecoder::new_text(1, multi_byte_options.clone()).unwrap()
    });
}

#[test]
fn text_accepts_one_consistent_record_ending_even_across_chunks() {
    let crlf = b"a\tb\r\nc\td\r\n";
    let expected = vec![
        vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
        vec![Some(b"c".to_vec()), Some(b"d".to_vec())],
    ];
    assert_all_two_way_splits(crlf, &expected, || {
        CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap()
    });

    let unterminated_after_crlf = b"a\tb\r\nc\td";
    assert_all_two_way_splits(unterminated_after_crlf, &expected, || {
        CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap()
    });

    let cr = b"a\tb\rc\td\r";
    assert_bytewise(cr, &expected, || {
        CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap()
    });

    let mut immediate_cr = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert_eq!(
        immediate_cr.push(copy_data(b"a\tb\rc\td")).unwrap().len(),
        1
    );
    assert_eq!(immediate_cr.push(copy_data(b"\r")).unwrap().len(), 1);
    assert!(immediate_cr.finish().unwrap().is_empty());
}

#[test]
fn mixed_record_endings_are_rejected_across_copy_data_messages() {
    let mut text = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert_eq!(text.push(copy_data(b"a\tb\r\n")).unwrap().len(), 1);
    assert!(text.push(copy_data(b"c\td\n")).is_err());
    assert!(text.push(copy_data(b"")).is_err());
    assert!(text.finish().is_err());

    let mut csv = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert_eq!(csv.push(copy_data(b"\"a\nb\",c\r\n")).unwrap().len(), 1);
    assert!(csv.push(copy_data(b"d,e\n")).is_err());
    assert!(csv.push(copy_data(b"")).is_err());
    assert!(csv.finish().is_err());

    let mut text = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert!(text.push(copy_data(b"a\tb\rc\td\r\n")).is_err());
    assert!(text.push(copy_data(b"")).is_err());
    assert!(text.finish().is_err());

    let mut csv = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert!(csv.push(copy_data(b"a,b\rc,d\r\n")).is_err());
    assert!(csv.push(copy_data(b"")).is_err());
    assert!(csv.finish().is_err());
}

#[test]
fn text_rejects_mixed_endings_bad_counts_and_incomplete_escapes() {
    let mut mixed = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert!(mixed.push(copy_data(b"a\tb\r\nc\td\n")).is_err());
    assert!(mixed.push(copy_data(b"e\tf\n")).is_err());
    assert!(mixed.finish().is_err());

    for input in [b"only-one\n".as_slice(), b"one\ttwo\tthree\n".as_slice()] {
        let mut decoder = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
        assert!(decoder.push(copy_data(input)).is_err());
        assert!(decoder.push(copy_data(b"")).is_err());
        assert!(decoder.finish().is_err());
    }

    let mut unterminated = CopyDecoder::new_text(2, CopyTextOptions::default()).unwrap();
    assert!(
        unterminated
            .push(copy_data(b"only-one"))
            .unwrap()
            .is_empty()
    );
    assert!(unterminated.finish().is_err());

    let mut dangling = CopyDecoder::new_text(1, CopyTextOptions::default()).unwrap();
    assert!(dangling.push(copy_data(b"value\\")).unwrap().is_empty());
    assert!(dangling.finish().is_err());
}

#[test]
fn csv_decoding_is_chunk_boundary_invariant() {
    let options = CopyCsvOptions {
        null_string: "NULL".into(),
        ..Default::default()
    };
    let input = b"one,\"two,2\",\"line\nbreak\"\nNULL,\"NULL\",plain\nlast,\"a\"\"b\",tail";
    let expected = vec![
        vec![
            Some(b"one".to_vec()),
            Some(b"two,2".to_vec()),
            Some(b"line\nbreak".to_vec()),
        ],
        vec![None, Some(b"NULL".to_vec()), Some(b"plain".to_vec())],
        vec![
            Some(b"last".to_vec()),
            Some(b"a\"b".to_vec()),
            Some(b"tail".to_vec()),
        ],
    ];

    assert_all_two_way_splits(input, &expected, || {
        CopyDecoder::new_csv(3, options.clone()).unwrap()
    });
    assert_bytewise(input, &expected, || {
        CopyDecoder::new_csv(3, options.clone()).unwrap()
    });

    let final_quote_at_eof = b"a,\"b\"";
    let final_quote_expected = vec![vec![Some(b"a".to_vec()), Some(b"b".to_vec())]];
    assert_all_two_way_splits(final_quote_at_eof, &final_quote_expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });
}

#[test]
fn csv_distinguishes_quoted_empty_and_null_fields() {
    let input = b",\"\",value\n\"\",,\"value\"\n";
    let rows = decode_chunks(
        CopyDecoder::new_csv(3, CopyCsvOptions::default()).unwrap(),
        input,
        &[1, 2, 5, 8, 12],
    );
    assert_eq!(
        rows,
        vec![
            vec![None, Some(vec![]), Some(b"value".to_vec())],
            vec![Some(vec![]), None, Some(b"value".to_vec())],
        ]
    );

    let custom_null = CopyCsvOptions {
        null_string: "NULL".into(),
        ..Default::default()
    };
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_csv(5, custom_null).unwrap(),
            b",\"\",NULL,\"NULL\",NU\"\"LL\n",
            &[1, 3, 8, 15],
        ),
        vec![vec![
            Some(vec![]),
            Some(vec![]),
            None,
            Some(b"NULL".to_vec()),
            Some(b"NULL".to_vec()),
        ]]
    );
}

#[test]
fn csv_supports_distinct_quote_and_escape_bytes() {
    let options = CopyCsvOptions {
        delimiter: "|".into(),
        quote: "'".into(),
        escape: "\\".into(),
        null_string: "NULL".into(),
        force_quote: vec![0, 1],
    };
    let input = b"'a\\'b'|'c\\\\d'|'a\\|b'|'ab''cd'|NULL\n";
    let expected = vec![vec![
        Some(b"a'b".to_vec()),
        Some(b"c\\d".to_vec()),
        Some(b"a\\|b".to_vec()),
        Some(b"abcd".to_vec()),
        None,
    ]];
    assert_all_two_way_splits(input, &expected, || {
        CopyDecoder::new_csv(5, options.clone()).unwrap()
    });

    let escape_in_null = CopyCsvOptions {
        delimiter: "|".into(),
        quote: "'".into(),
        escape: "\\".into(),
        null_string: "\\N".into(),
        ..Default::default()
    };
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_csv(2, escape_in_null).unwrap(),
            b"\\N|value\n",
            &[1, 3],
        ),
        vec![vec![None, Some(b"value".to_vec())]]
    );
}

#[test]
fn csv_escape_is_special_only_inside_quoted_sections() {
    let options = CopyCsvOptions {
        delimiter: "|".into(),
        quote: "'".into(),
        escape: "\\".into(),
        null_string: "NULL".into(),
        force_quote: vec![],
    };
    let input = b"outside\\q|'inside\\q'\n";
    let expected = vec![vec![
        Some(b"outside\\q".to_vec()),
        Some(b"inside\\q".to_vec()),
    ]];
    assert_bytewise(input, &expected, || {
        CopyDecoder::new_csv(2, options.clone()).unwrap()
    });
}

#[test]
fn csv_ignores_force_quote_while_decoding_unquoted_fields() {
    let options = CopyCsvOptions {
        delimiter: "|".into(),
        quote: "'".into(),
        escape: "\\".into(),
        null_string: "NULL".into(),
        force_quote: vec![usize::MAX, usize::MAX, 42],
    };
    let rows = decode_chunks(
        CopyDecoder::new_csv(2, options).unwrap(),
        b"left|right\n",
        &[0, 3, 5],
    );
    assert_eq!(
        rows,
        vec![vec![Some(b"left".to_vec()), Some(b"right".to_vec())]]
    );
}

#[test]
fn csv_supports_quoted_sections_with_surrounding_unquoted_bytes() {
    let input = b"before\"inside,field\"after,plain\nleft\"\"right,value\n\\.,tail\n";
    let expected = vec![
        vec![
            Some(b"beforeinside,fieldafter".to_vec()),
            Some(b"plain".to_vec()),
        ],
        vec![Some(b"leftright".to_vec()), Some(b"value".to_vec())],
        vec![Some(b"\\.".to_vec()), Some(b"tail".to_vec())],
    ];
    assert_all_two_way_splits(input, &expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });
}

#[test]
fn csv_preserves_embedded_crlf_and_handles_split_crlf_boundaries() {
    let input = b"\"a\r\nb\",c\r\n\"d\",e\r\n";
    let expected = vec![
        vec![Some(b"a\r\nb".to_vec()), Some(b"c".to_vec())],
        vec![Some(b"d".to_vec()), Some(b"e".to_vec())],
    ];
    assert_all_two_way_splits(input, &expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });
    assert_bytewise(input, &expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });

    let unterminated_after_crlf = b"a,b\r\nc,d";
    let plain_expected = vec![
        vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
        vec![Some(b"c".to_vec()), Some(b"d".to_vec())],
    ];
    assert_all_two_way_splits(unterminated_after_crlf, &plain_expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });

    let cr = b"\"a\rb\",c\r\"d\",e\r";
    let cr_expected = vec![
        vec![Some(b"a\rb".to_vec()), Some(b"c".to_vec())],
        vec![Some(b"d".to_vec()), Some(b"e".to_vec())],
    ];
    assert_all_two_way_splits(cr, &cr_expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });
    assert_bytewise(cr, &cr_expected, || {
        CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap()
    });

    let mut immediate_cr = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert_eq!(immediate_cr.push(copy_data(b"a,b\rc,d")).unwrap().len(), 1);
    assert_eq!(immediate_cr.push(copy_data(b"\r")).unwrap().len(), 1);
    assert!(immediate_cr.finish().unwrap().is_empty());
}

#[test]
fn csv_rejects_bad_counts_unclosed_fields_and_mixed_endings() {
    for input in [b"one\n".as_slice(), b"one,two,three\n".as_slice()] {
        let mut decoder = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
        assert!(
            decoder.push(copy_data(input)).is_err(),
            "accepted {input:?}"
        );
        assert!(decoder.push(copy_data(b"")).is_err());
        assert!(decoder.finish().is_err());
    }

    let mut unterminated = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert!(
        unterminated
            .push(copy_data(b"only-one"))
            .unwrap()
            .is_empty()
    );
    assert!(unterminated.finish().is_err());

    let mut unclosed = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert!(
        unclosed
            .push(copy_data(b"one,two\"three"))
            .unwrap()
            .is_empty()
    );
    assert!(unclosed.finish().is_err());

    let mut mixed = CopyDecoder::new_csv(2, CopyCsvOptions::default()).unwrap();
    assert!(mixed.push(copy_data(b"a,b\r\nc,d\n")).is_err());
    assert!(mixed.push(copy_data(b"")).is_err());
    assert!(mixed.finish().is_err());
}

#[test]
fn binary_decoding_is_chunk_boundary_invariant() {
    let mut input = binary_header(0x0000_8002, b"future");
    put_binary_row(&mut input, &[Some(&[0, 0, 0, 42]), None]);
    put_binary_row(&mut input, &[Some(&[]), Some(b"abc")]);
    input.put_i16(-1);

    let expected = vec![
        vec![Some(vec![0, 0, 0, 42]), None],
        vec![Some(vec![]), Some(b"abc".to_vec())],
    ];
    let mut immediate = CopyDecoder::new_binary(2).unwrap();
    assert_eq!(
        immediate
            .push(CopyData::new(input.clone().freeze()))
            .unwrap()
            .into_iter()
            .map(unpack_row)
            .collect::<Values>(),
        expected
    );
    assert!(immediate.finish().unwrap().is_empty());

    assert_all_two_way_splits(&input, &expected, || CopyDecoder::new_binary(2).unwrap());
    assert_bytewise(&input, &expected, || CopyDecoder::new_binary(2).unwrap());

    let many_fields = vec![Some(b"".as_slice()); 256];
    let mut wide_tuple = binary_header(0, b"");
    put_binary_row(&mut wide_tuple, &many_fields);
    wide_tuple.put_i16(-1);
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_binary(256).unwrap(),
            &wide_tuple,
            &[19, 20, 21, 22, wide_tuple.len() - 2],
        ),
        vec![vec![Some(Vec::new()); 256]]
    );

    let wide_value = vec![0xA5; u16::MAX as usize + 1];
    let mut wide_length = binary_header(0, b"");
    put_binary_row(&mut wide_length, &[Some(&wide_value)]);
    wide_length.put_i16(-1);
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_binary(1).unwrap(),
            &wide_length,
            &[19, 21, 23, 25, 25 + u16::MAX as usize],
        ),
        vec![vec![Some(wide_value)]]
    );
}

#[test]
fn binary_header_and_trailer_can_describe_an_empty_stream() {
    for flags in [0, 1, 0x7FFF, 0x8000, 0xFFFF] {
        let mut input = binary_header(flags, b"");
        input.put_i16(-1);
        assert_eq!(
            decode_chunks(CopyDecoder::new_binary(3).unwrap(), &input, &[1, 11, 18]),
            Values::new(),
            "low flag mask {flags:#06x} should be ignored"
        );
    }

    let extension = vec![0xA5; u16::MAX as usize + 1];
    let mut input = binary_header(0, &extension);
    input.put_i16(-1);
    assert_eq!(
        decode_chunks(
            CopyDecoder::new_binary(1).unwrap(),
            &input,
            &[11, 15, 18, 19, 19 + u16::MAX as usize, input.len() - 2],
        ),
        Values::new()
    );
}

#[test]
fn binary_rejects_bad_headers_critical_flags_and_extension_lengths() {
    let mut bad_signature = binary_header(0, b"");
    bad_signature[0] = b'X';
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(decoder.push(CopyData::new(bad_signature.freeze())).is_err());

    let mut critical = binary_header(0x0001_0000, b"");
    critical.put_i16(-1);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(decoder.push(CopyData::new(critical.freeze())).is_err());

    let mut other_critical = binary_header(i32::MIN, b"");
    other_critical.put_i16(-1);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(CopyData::new(other_critical.freeze()))
            .is_err()
    );

    let mut negative_extension = binary_header(0, b"");
    negative_extension[15..19].copy_from_slice(&(-1i32).to_be_bytes());
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(CopyData::new(negative_extension.freeze()))
            .is_err()
    );

    let incomplete_extension = binary_header(0, b"abc");
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(copy_data(
                &incomplete_extension[..incomplete_extension.len() - 1]
            ))
            .unwrap()
            .is_empty()
    );
    assert!(decoder.finish().is_err());
}

#[test]
fn binary_rejects_bad_counts_lengths_truncation_and_missing_trailer() {
    let mut invalid_trailer = binary_header(0, b"");
    invalid_trailer.put_i16(-2);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(CopyData::new(invalid_trailer.freeze()))
            .is_err()
    );
    assert!(decoder.push(copy_data(&(-1i16).to_be_bytes())).is_err());
    assert!(decoder.finish().is_err());

    let mut wrong_count = binary_header(0, b"");
    wrong_count.put_i16(1);
    let mut decoder = CopyDecoder::new_binary(2).unwrap();
    assert!(decoder.push(CopyData::new(wrong_count.freeze())).is_err());
    assert!(decoder.push(copy_data(&(-1i16).to_be_bytes())).is_err());
    assert!(decoder.finish().is_err());

    let mut over_count = binary_header(0, b"");
    over_count.put_i16(2);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(decoder.push(CopyData::new(over_count.freeze())).is_err());
    assert!(decoder.push(copy_data(&(-1i16).to_be_bytes())).is_err());
    assert!(decoder.finish().is_err());

    let mut bad_length = binary_header(0, b"");
    bad_length.put_i16(1);
    bad_length.put_i32(-2);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(decoder.push(CopyData::new(bad_length.freeze())).is_err());
    let mut recovery = BytesMut::new();
    recovery.put_i32(0);
    recovery.put_i16(-1);
    assert!(decoder.push(CopyData::new(recovery.freeze())).is_err());
    assert!(decoder.finish().is_err());

    let mut truncated = binary_header(0, b"");
    truncated.put_i16(1);
    truncated.put_i32(4);
    truncated.extend_from_slice(&[1, 2]);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(CopyData::new(truncated.freeze()))
            .unwrap()
            .is_empty()
    );
    assert!(decoder.finish().is_err());

    let mut no_trailer = binary_header(0, b"");
    put_binary_row(&mut no_trailer, &[Some(b"value")]);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert_eq!(
        decoder
            .push(CopyData::new(no_trailer.freeze()))
            .unwrap()
            .len(),
        1
    );
    assert!(decoder.finish().is_err());
}

#[test]
fn binary_rejects_data_after_the_trailer_and_terminalizes() {
    let mut same_chunk = binary_header(0, b"");
    same_chunk.put_i16(-1);
    put_binary_row(&mut same_chunk, &[Some(&[])]);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(decoder.push(CopyData::new(same_chunk.freeze())).is_err());
    assert!(decoder.push(copy_data(b"")).is_err());
    assert!(decoder.finish().is_err());

    let mut header_and_trailer = binary_header(0, b"");
    header_and_trailer.put_i16(-1);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(CopyData::new(header_and_trailer.freeze()))
            .unwrap()
            .is_empty()
    );
    assert!(decoder.push(copy_data(b"")).is_err());
    assert!(decoder.finish().is_err());

    let mut header_and_trailer = binary_header(0, b"");
    header_and_trailer.put_i16(-1);
    let mut decoder = CopyDecoder::new_binary(1).unwrap();
    assert!(
        decoder
            .push(CopyData::new(header_and_trailer.freeze()))
            .unwrap()
            .is_empty()
    );
    assert!(decoder.finish().unwrap().is_empty());
    assert!(decoder.finish().is_err());
    assert!(decoder.push(copy_data(b"later")).is_err());
}

#[test]
fn finish_rejects_every_incomplete_binary_phase() {
    let mut untouched = CopyDecoder::new_binary(1).unwrap();
    assert!(untouched.finish().is_err());
    assert!(untouched.push(copy_data(b"")).is_err());
    assert!(untouched.finish().is_err());

    let complete_header = binary_header(0, b"");
    let cases = vec![
        Vec::new(),
        b"PG".to_vec(),
        complete_header[..13].to_vec(),
        complete_header[..18].to_vec(),
        complete_header.to_vec(),
        {
            let mut data = complete_header.to_vec();
            data.push(0);
            data
        },
        {
            let mut data = complete_header.to_vec();
            data.extend_from_slice(&1i16.to_be_bytes());
            data.extend_from_slice(&4i32.to_be_bytes()[..2]);
            data
        },
    ];

    for input in cases {
        let mut decoder = CopyDecoder::new_binary(1).unwrap();
        assert!(decoder.push(copy_data(&input)).unwrap().is_empty());
        assert!(decoder.finish().is_err(), "accepted incomplete {input:?}");
    }
}

#[test]
fn copy_encoder_output_round_trips_through_the_decoder() {
    let binary_schema = Arc::new(vec![
        FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Binary),
        FieldInfo::new(
            "name".into(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Binary,
        ),
    ]);
    let mut binary_encoder = CopyEncoder::new_binary(binary_schema);
    binary_encoder.encode_field(&42i32).unwrap();
    binary_encoder.encode_field(&None::<String>).unwrap();
    let first = binary_encoder.take_copy();
    binary_encoder.encode_field(&7i32).unwrap();
    binary_encoder.encode_field(&"seven").unwrap();
    let second = binary_encoder.take_copy();

    let mut binary_decoder = CopyDecoder::new_binary(2).unwrap();
    let mut rows = binary_decoder.push(first).unwrap();
    rows.extend(binary_decoder.push(second).unwrap());
    rows.extend(
        binary_decoder
            .push(CopyEncoder::finish_copy_binary())
            .unwrap(),
    );
    rows.extend(binary_decoder.finish().unwrap());
    assert_eq!(
        rows.into_iter().map(unpack_row).collect::<Values>(),
        vec![
            vec![Some(42i32.to_be_bytes().to_vec()), None],
            vec![Some(7i32.to_be_bytes().to_vec()), Some(b"seven".to_vec()),],
        ]
    );

    let text_options = CopyTextOptions {
        delimiter: "|".into(),
        null_string: "\\".into(),
    };
    let text_schema = Arc::new(vec![
        FieldInfo::new("value".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("other".into(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]);
    let mut text_encoder = CopyEncoder::new_text(text_schema, text_options.clone());
    text_encoder.encode_field(&"a|b\nc").unwrap();
    text_encoder.encode_field(&None::<String>).unwrap();
    let text_chunk = text_encoder.take_copy();
    let mut text_decoder = CopyDecoder::new_text(2, text_options).unwrap();
    let mut text_rows = text_decoder.push(text_chunk).unwrap();
    text_rows.extend(text_decoder.finish().unwrap());
    assert_eq!(
        text_rows.into_iter().map(unpack_row).collect::<Values>(),
        vec![vec![Some(b"a|b\nc".to_vec()), None]]
    );

    let csv_options = CopyCsvOptions {
        null_string: "NULL".into(),
        force_quote: vec![0, 1],
        ..Default::default()
    };
    let csv_schema = Arc::new(vec![
        FieldInfo::new("value".into(), None, None, Type::VARCHAR, FieldFormat::Text),
        FieldInfo::new("other".into(), None, None, Type::VARCHAR, FieldFormat::Text),
    ]);
    let mut csv_encoder = CopyEncoder::new_csv(csv_schema, csv_options.clone());
    csv_encoder.encode_field(&"a,\"b\nc").unwrap();
    csv_encoder.encode_field(&None::<String>).unwrap();
    let csv_chunk = csv_encoder.take_copy();
    let mut csv_decoder = CopyDecoder::new_csv(2, csv_options).unwrap();
    let mut csv_rows = csv_decoder.push(csv_chunk).unwrap();
    csv_rows.extend(csv_decoder.finish().unwrap());
    assert_eq!(
        csv_rows.into_iter().map(unpack_row).collect::<Values>(),
        vec![vec![Some(b"a,\"b\nc".to_vec()), None]]
    );
}
