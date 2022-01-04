import csv
import re

import digital_land


class HarmoniserPlugin:
    organisation_uri = {}
    end_of_uri_regex = re.compile(r".*/")

    @digital_land.hookimpl
    def init_harmoniser_plugin(self, harmoniser):
        self.harmoniser = harmoniser

        for row in csv.DictReader(open("var/cache/organisation.csv", newline="")):
            if "opendatacommunities" in row:
                uri = row["opendatacommunities"].lower()
                self.organisation_uri[row["organisation"].lower()] = uri
                self.organisation_uri[uri] = uri
                self.organisation_uri[self.end_of_uri(uri)] = uri
                self.organisation_uri[row["statistical-geography"].lower()] = uri
                if "local-authority-eng" in row["organisation"]:
                    dl_url = "https://digital-land.github.io/organisation/%s/" % (
                        row["organisation"]
                    )
                    dl_url = dl_url.lower().replace("-eng:", "-eng/")
                    self.organisation_uri[dl_url] = uri

        self.organisation_uri.pop("")

    @digital_land.hookimpl
    def apply_patch_post(self, fieldname, value):
        if fieldname == "OrganisationURI":
            normalised_value = self.lower_uri(value)

            if normalised_value in self.organisation_uri:
                return self.organisation_uri[normalised_value]

            s = self.end_of_uri(normalised_value)
            if s in self.organisation_uri:
                return self.organisation_uri[s]

            self.harmoniser.log_issue(
                fieldname, "opendatacommunities-uri", normalised_value
            )
        return value

    @digital_land.hookimpl
    def set_resource_defaults_post(self):
        if "entry-date" in self.harmoniser.default_values:
            self.harmoniser.default_values[
                "LastUpdatedDate"
            ] = self.harmoniser.default_values["entry-date"]

        if "organisation" in self.harmoniser.default_values:
            key = self.harmoniser.default_values["organisation"].lower()
            if key in self.organisation_uri:
                self.harmoniser.default_values["OrganisationURI"] = self.organisation_uri[key]

    def lower_uri(self, value):
        return "".join(value.split()).lower()

    def end_of_uri(self, value):
        return self.end_of_uri_regex.sub("", value.rstrip("/").lower())


# regsiter plugin instances, not the classes themselves
harmoniser_plugin = HarmoniserPlugin()
