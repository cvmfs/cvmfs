package main

type StoredObjectRecord struct {
	S3 struct {
		Bucket struct {
			Name string
		}
		Object struct {
			Key string
		}
	}
}

type WebhookPayload struct {
	EventType string
	Records   []StoredObjectRecord
}

type PublishingObject struct {
	Bucket string
	Key    string
}

func (p *WebhookPayload) Object() PublishingObject {
	s3 := p.Records[0].S3

	return PublishingObject{
		Bucket: s3.Bucket.Name,
		Key:    s3.Object.Key,
	}
}

type PublisherConfig struct {
	fqrn             string
	localPayloadPath string
}

type StatusRequest struct {
	Key    string
	Status string
}
