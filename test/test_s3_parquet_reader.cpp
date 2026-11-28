// Copyright 2019-2026 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <memory>
#include <string>

#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/uri.h>
#include <ygm/comm.hpp>

namespace {

// AWS uses an access key ID (the API key) and a secret access key.
// Prefer environment variables over replacing these placeholders with secrets.
const std::string access_key_placeholder = "<placeholder key>";
const std::string secret_key_placeholder = "<placeholder key>";

// S3_ENDPOINT_URL overrides this fallback. Empty selects the AWS endpoint.
const std::string endpoint_placeholder = "";
const std::string default_region       = "us-east-1";

std::string environment_or(const char* name, const std::string& fallback) {
  const char* value = std::getenv(name);
  return value != nullptr && value[0] != '\0' ? std::string(value) : fallback;
}

// Creating the filesystem alone does not verify access. GetFileInfo on a
// bucket-only path performs a bucket metadata request without reading objects.
arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> check_bucket(
    const std::string& bucket, const arrow::fs::S3Options& options) {
  auto filesystem_result = arrow::fs::S3FileSystem::Make(options);
  if (!filesystem_result.ok()) {
    return filesystem_result.status();
  }

  auto filesystem = *filesystem_result;
  // Arrow S3 paths are "bucket/key", not "s3://bucket/key".
  auto info_result = filesystem->GetFileInfo(bucket);
  if (!info_result.ok()) {
    return info_result.status();
  }
  if (!info_result->IsDirectory()) {
    return arrow::Status::IOError("S3 bucket was not found: ", bucket);
  }

  return filesystem;
}

// The caller must initialize S3 and keep it active until the filesystem is
// destroyed. Return the filesystem so it can be used for subsequent requests.
arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> connect_to_bucket(
    const std::string& bucket, const std::string& region,
    const std::string& endpoint_url) {
  const std::string access_key =
      environment_or("S3_ACCESS_KEY_ID", access_key_placeholder);
  const std::string secret_key =
      environment_or("S3_SECRET_ACCESS_KEY", secret_key_placeholder);
  const std::string session_token = environment_or("S3_SESSION_TOKEN", "");

  if (access_key == access_key_placeholder ||
      secret_key == secret_key_placeholder) {
    return arrow::Status::Invalid(
        "Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY, or replace the "
        "credential placeholders. Temporary credentials also require "
        "S3_SESSION_TOKEN.");
  }

  auto options   = arrow::fs::S3Options::FromAccessKey(access_key, secret_key,
                                                       session_token);
  options.region = region;

  if (!endpoint_url.empty()) {
    // Arrow expects the scheme separately from the endpoint host/port.
    arrow::util::Uri endpoint;
    const auto       status = endpoint.Parse(endpoint_url);
    if (!status.ok()) {
      return status;
    }
    if ((endpoint.scheme() != "https" && endpoint.scheme() != "http") ||
        endpoint.host().empty() ||
        (!endpoint.path().empty() && endpoint.path() != "/") ||
        !endpoint.query_string().empty() || !endpoint.username().empty() ||
        !endpoint.password().empty()) {
      return arrow::Status::Invalid(
          "The endpoint must be an HTTP or HTTPS URL containing only a host "
          "and optional port, without credentials, a bucket path, or a query.");
    }

    options.scheme            = endpoint.scheme();
    options.endpoint_override = arrow::util::UriEncodeHost(endpoint.host());
    if (!endpoint.port_text().empty()) {
      options.endpoint_override += ":" + endpoint.port_text();
    }
    // Use path-style bucket addressing for a custom endpoint.
    options.force_virtual_addressing = false;
  }

  options.connect_timeout = 5.0;
  options.retry_strategy =
      arrow::fs::S3RetryStrategy::GetAwsStandardRetryStrategy(2);
  return check_bucket(bucket, options);
}

}  // namespace

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  const std::string endpoint_url =
      environment_or("S3_ENDPOINT_URL", endpoint_placeholder);
  const std::string endpoint_description =
      endpoint_url.empty() ? "AWS endpoint selected from region" : endpoint_url;

  const bool help = argc == 2 && std::string(argv[1]) == "--help";
  if (help || argc < 2 || argc > 3) {
    world.cout0("Usage: ", argv[0], " <bucket-name> [aws-region]");
    world.cout0("S3 endpoint: ", endpoint_description);
    world.cout0("Set S3_ENDPOINT_URL to override the endpoint placeholder. ",
                "An empty fallback uses the AWS endpoint for the region.");
    world.cout0("Default region: ", default_region);
    world.cout0("Rank 0 lists all objects recursively after checking access.");
    world.cout0("Supply a bucket name without s3:// or an object path. ",
                "Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY; ",
                "set S3_SESSION_TOKEN for temporary credentials.");
    return help ? EXIT_SUCCESS : EXIT_FAILURE;
  }

  const std::string bucket = argv[1];
  const std::string region = argc == 3 ? argv[2] : default_region;
  if (bucket.empty() || bucket.find('/') != std::string::npos ||
      region.empty()) {
    world.cerr0("Provide a nonempty bucket name without s3:// or slashes, ",
                "and a nonempty AWS region.");
    return EXIT_FAILURE;
  }

  bool success = true;
  if (world.rank0()) {
    auto global_options      = arrow::fs::S3GlobalOptions::Defaults();
    global_options.log_level = arrow::fs::S3LogLevel::Off;
    auto status              = arrow::fs::InitializeS3(global_options);
    if (status.ok()) {
      try {
        auto filesystem_result =
            connect_to_bucket(bucket, region, endpoint_url);
        if (!filesystem_result.ok()) {
          status = filesystem_result.status();
        } else {
          auto filesystem = *filesystem_result;
          world.cout0("Successfully accessed S3 bucket ", bucket, " at ",
                      endpoint_description, " in region ", region, ".");

          arrow::fs::FileSelector selector;
          selector.base_dir        = bucket;
          selector.recursive       = true;
          selector.allow_not_found = false;
          auto objects_result      = filesystem->GetFileInfo(selector);
          if (!objects_result.ok()) {
            status = objects_result.status();
          } else {
            auto& objects = *objects_result;
            std::sort(objects.begin(), objects.end(),
                      arrow::fs::FileInfo::ByPath());
            size_t object_count{0};
            for (const auto& object : objects) {
              // Arrow also returns synthetic directories for S3 prefixes.
              if (object.IsFile()) {
                world.cout0("s3://", object.path(), " (", object.size(),
                            " bytes)");
                ++object_count;
              }
            }
            world.cout0("Listed ", object_count,
                        " objects. No Parquet data was read.");
          }
        }
      } catch (const std::exception& error) {
        status = arrow::Status::UnknownError(error.what());
      }

      // All filesystem handles above are destroyed before S3 is finalized.
      const auto finalize_status = arrow::fs::FinalizeS3();
      if (status.ok()) {
        status = finalize_status;
      }
    }

    success = status.ok();
    if (!success) {
      world.cerr0("S3 bucket access or listing failed: ", status.ToString());
    }
  }

  // Other ranks wait for access checking and listing, then share the exit
  // status.
  return ygm::logical_and(success, world) ? EXIT_SUCCESS : EXIT_FAILURE;
}
