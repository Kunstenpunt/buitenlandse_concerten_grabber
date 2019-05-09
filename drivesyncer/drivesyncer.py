from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys


class DriveSyncer(object):
    def __init__(self):
        # connect with google drive
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("drivesyncer/resources/credentials.json")
        gauth.SaveCredentialsFile("drivesyncer/resources/credentials.json")
        self.drive = GoogleDrive(gauth)
        self.resource_files = [
            "belgian_mscbrnz_artists.xlsx",
            "city_cleaning.xlsx",
            "country_cleaning.xlsx",
            "venue_cleaning.xlsx",
            "ignore_list.xlsx",
            "grace_list.xlsx",
            "manual.xlsx",
            "merge_artists.xlsx"
        ]
        self.resource_files_gdrive_ids = None

    def set_resource_files_ids(self):
        self.resource_files_gdrive_ids = [(resource_file, self.get_google_drive_id(resource_file)) for resource_file in self.resource_files]

    def get_google_drive_id(self, filename):
        print("\t", filename)
        return self.drive.ListFile({'q': "'1cLSv4xT3b3rrGwZo7Ke1BxkZSHV5QboU' in parents and title='{0}' and trashed=false".format(filename)}).GetList()[0]["id"]

    def get_google_drive_link(self, filename):
        return self.drive.ListFile({'q': "'1cLSv4xT3b3rrGwZo7Ke1BxkZSHV5QboU' in parents and title='{0}' and trashed=false".format(filename)}).GetList()[0]["alternateLink"]

    def upload_file_to_leeches(self, filename, fid):
        file = self.drive.CreateFile({'title': filename, 'id': fid})
        file.SetContentFile(filename)
        file['parents'] = [{"kind": "drive#fileLink", "id": '1cLSv4xT3b3rrGwZo7Ke1BxkZSHV5QboU'}]
        file.Upload(param={"convert": True})

    def downstream(self):
        for item in self.resource_files_gdrive_ids:
            self.update_local_resource(item[0], item[1])
        self.update_local_latest()

    def upstream(self):
        for item in self.resource_files_gdrive_ids:
            self.update_remote_resource(item[0], item[1])
        self.update_remote_latest()

    def update_local_resource(self, filename, fid):
        file = self.drive.CreateFile({"id": fid})
        file.GetContentFile("resources/{0}".format(filename), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def update_local_latest(self):
        file = self.drive.CreateFile({"id": "0B4I3gofjeGMHWUk5bmZvQm5fdFk"})
        file.GetContentFile("output/latest.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def update_remote_resource(self, filename, fid):
        file = self.drive.CreateFile({'title': filename, 'id': fid})
        file.SetContentFile("resources/" + filename)
        file['parents'] = [{"kind": "drive#fileLink", "id": '1cLSv4xT3b3rrGwZo7Ke1BxkZSHV5QboU'}]
        file.Upload(param={"convert": True})

    def update_remote_latest(self):
        file = self.drive.CreateFile({'title': "latest.xlsx", 'id': "0B4I3gofjeGMHWUk5bmZvQm5fdFk"})
        file.SetContentFile("output/latest.xlsx")
        file['parents'] = [{"kind": "drive#fileLink", "id": '1cLSv4xT3b3rrGwZo7Ke1BxkZSHV5QboU'}]
        file.Upload(param={"convert": True})


if __name__ == "__main__":
    syncdrive = DriveSyncer
    if sys.argv[1] == "downstream":
        syncdrive.set_resource_files_ids()
        syncdrive.downstream()

    if sys.argv[1] == "upstream":
        syncdrive.upstream()
