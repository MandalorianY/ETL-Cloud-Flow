from google.cloud import dataproc_v1 as dataproc


def dataproc_activation(event, context):
    """Pusub Cloud Function that activates a Dataproc Workflow Template."""
    region = 'europe-west3'
    project_id = 'nodale'
    template_id = "ncis-workflow-template"
    parent = f'projects/{project_id}/regions/{region}'

    client = dataproc.WorkflowTemplateServiceClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    # Instantiate the workflow template
    response = client.instantiate_workflow_template(
        name=f'{parent}/workflowTemplates/{template_id}')

    return (f'Template {template_id} instantiated: {response}')
