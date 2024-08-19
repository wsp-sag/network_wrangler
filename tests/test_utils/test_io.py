"""Module for testing the utils.io module."""

from network_wrangler.utils.io import convert_file_serialization


@pytest.mark.skip(reason="Not implemented")
def test_convert_in_chunks(example_dir, tmpdir):
    convert_file_serialization(
        example_dir / "stpaul" / "link.json", tmpdir / "chunked_links.parquet", chunk_size=100
    )
    convert_file_serialization(
        example_dir / "stpaul" / "link.json", tmpdir / "not_chunked_links.parquet"
    )
    # make sure they are not empty
    assert (tmpdir / "chunked_links.parquet").stat().st_size > 0
    assert (tmpdir / "not_chunked_links.parquet").stat().st_size > 0

    # make sure they are the same size
    assert (tmpdir / "chunked_links.parquet").stat().st_size == (
        tmpdir / "not_chunked_links.parquet"
    ).stat().st_size

    # make sure they are the same
    assert (tmpdir / "chunked_links.parquet").read_bytes() == (
        tmpdir / "not_chunked_links.parquet"
    ).read_bytes()

    # clean up
    (tmpdir / "chunked_links.parquet").unlink()
    (tmpdir / "not_chunked_links.parquet").unlink()
