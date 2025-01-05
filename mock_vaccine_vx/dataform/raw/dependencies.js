// Set your project and dataset
var project_name = "sidata-test";
var dataset_name = "mock_vaccine_vx";

// List all table/view names
var sources = [
  "person",
  "employee",
  "vaccination",
  "vaccine",
  "location"
];

// Declare all tables/views in the dataset
sources.forEach(source => declare({
    database: project_name,
    schema: dataset_name,
    name: source
}));
