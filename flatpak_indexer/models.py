class RegistryModel:
    def __init__(self, repositories=None):
        self.repositories = repositories if repositories else {}

    def add_image(self, name, image):
        if name not in self.repositories:
            self.repositories[name] = RepositoryModel(name=name)

        self.repositories[name].images[image.digest] = image

    @classmethod
    def from_json(cls, data):
        return cls(repositories={r['Name']: RepositoryModel.from_json(r)
                                 for r in data['Repositories']})

    def to_json(self):
        return {
            'Repositories': [self.repositories[k].to_json() for k in sorted(self.repositories)]
        }


class RepositoryModel:
    def __init__(self, name, images=None, lists=None):
        self.name = name
        self.images = images if images else {}
        self.lists = lists if lists else {}

    @classmethod
    def from_json(cls, data):
        return cls(name=data['Name'],
                   images={x['Digest']: ImageModel.from_json(x)
                           for x in data.get('Images', ())},
                   lists={x['Digest']: ListModel.from_json(x)
                          for x in data.get('Lists', ())})

    def to_json(self):
        result = {
            'Name': self.name,
            'Images': [self.images[k].to_json() for k in sorted(self.images)],
            'Lists': [self.lists[k].to_json() for k in sorted(self.lists)],
        }

        return result


class ListModel:
    def __init__(self, digest, media_type, images, tags):
        self.digest = digest
        self.media_type = media_type
        self.images = images
        self.tags = tags

    @classmethod
    def from_json(cls, data):
        return cls(digest=data['Digest'],
                   media_type=data['MediaType'],
                   images=[ImageModel.from_json(i) for i in data['Images']],
                   tags=data['Tags'])

    def to_json(self):
        return {
            'Digest': self.digest,
            'MediaType': self.media_type,
            'Images': [x.to_json() for x in self.images],
            'Tags': self.tags,
        }


class ImageModel:
    def __init__(self, digest, media_type, os, architecture, labels, annotations, tags):
        self.digest = digest
        self.media_type = media_type
        self.os = os
        self.architecture = architecture
        self.labels = labels
        self.annotations = annotations
        self.tags = tags

    @classmethod
    def from_json(cls, data):
        return cls(digest=data['Digest'],
                   media_type=data['MediaType'],
                   os=data['OS'],
                   architecture=data['Architecture'],
                   labels=data['Labels'],
                   annotations=data['Annotations'],
                   tags=data['Tags'])

    def to_json(self):
        return {
            'Digest': self.digest,
            'MediaType': self.media_type,
            'OS': self.os,
            'Architecture': self.architecture,
            'Labels': self.labels,
            'Annotations': self.annotations,
            'Tags': self.tags,
        }
