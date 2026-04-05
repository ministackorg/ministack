import { useCallback, useEffect, useState } from 'react'
import { fetchS3Buckets, fetchS3Objects, fetchS3Object, getS3DownloadUrl } from '@/lib/api'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from '@/components/ui/sheet'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { Separator } from '@/components/ui/separator'
import { EmptyState } from '@/components/EmptyState'
import { JsonViewer } from '@/components/JsonViewer'
import { useFetch } from '@/hooks/useFetch'
import {
  HardDrive,
  Folder,
  File,
  FileText,
  FileImage,
  FileCode,
  FileArchive,
  ChevronRight,
  ArrowLeft,
  Clock,
  Globe,
  Lock,
  Tag,
  Shield,
  Download,
} from 'lucide-react'

interface S3Bucket {
  name: string
  created: string
  region: string
  object_count: number
  total_size: number
  versioning: string
  encryption: string
  tags: Record<string, string>
}

interface S3File {
  key: string
  name: string
  size: number
  content_type: string
  etag: string
  last_modified: string
}

interface S3ObjectsResponse {
  bucket: string
  prefix: string
  delimiter: string
  folders: string[]
  files: S3File[]
}

interface S3ObjectDetail {
  bucket: string
  key: string
  size: number
  content_type: string
  content_encoding: string | null
  etag: string
  last_modified: string
  version_id: string | null
  metadata: Record<string, string>
  preserved_headers: Record<string, string>
  tags: Record<string, string>
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`
}

function formatDate(iso: string): string {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function getFileIcon(contentType: string, name: string) {
  if (contentType.startsWith('image/')) return FileImage
  if (contentType.startsWith('text/')) return FileText
  if (contentType.includes('json') || contentType.includes('xml') || contentType.includes('javascript') || contentType.includes('yaml')) return FileCode
  if (contentType.includes('zip') || contentType.includes('tar') || contentType.includes('gzip') || contentType.includes('compressed')) return FileArchive
  const ext = name.split('.').pop()?.toLowerCase()
  if (ext && ['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp', 'ico', 'bmp'].includes(ext)) return FileImage
  if (ext && ['js', 'ts', 'tsx', 'jsx', 'py', 'go', 'rs', 'java', 'rb', 'sh', 'css', 'html', 'yml', 'toml'].includes(ext)) return FileCode
  if (ext && ['zip', 'tar', 'gz', 'bz2', 'rar', '7z', 'jar', 'whl'].includes(ext)) return FileArchive
  if (ext && ['md', 'txt', 'csv', 'log', 'ini', 'cfg', 'conf'].includes(ext)) return FileText
  return File
}

export function S3Browser() {
  const bucketsFetcher = useCallback(() => fetchS3Buckets(), [])
  const { data: bucketsData, loading: bucketsLoading } = useFetch<{ buckets: S3Bucket[] }>(bucketsFetcher, 10000)

  const [selectedBucket, setSelectedBucket] = useState<string | null>(null)
  const [prefix, setPrefix] = useState('')
  const [objectsData, setObjectsData] = useState<S3ObjectsResponse | null>(null)
  const [loadingObjects, setLoadingObjects] = useState(false)
  const [objectDetail, setObjectDetail] = useState<S3ObjectDetail | null>(null)

  useEffect(() => {
    if (!selectedBucket) {
      setObjectsData(null)
      return
    }
    setLoadingObjects(true)
    fetchS3Objects(selectedBucket, prefix)
      .then(setObjectsData)
      .catch(() => setObjectsData(null))
      .finally(() => setLoadingObjects(false))
  }, [selectedBucket, prefix])

  const openObject = async (bucket: string, key: string) => {
    try {
      const data = await fetchS3Object(bucket, key)
      setObjectDetail(data)
    } catch {
      setObjectDetail(null)
    }
  }

  const navigateToFolder = (folderPrefix: string) => {
    setPrefix(folderPrefix)
  }

  const navigateUp = () => {
    const parts = prefix.replace(/\/$/, '').split('/')
    parts.pop()
    setPrefix(parts.length > 0 ? parts.join('/') + '/' : '')
  }

  const breadcrumbs = prefix ? prefix.replace(/\/$/, '').split('/') : []

  const buckets = bucketsData?.buckets ?? []

  // Bucket list view
  if (!selectedBucket) {
    if (bucketsLoading) {
      return (
        <div className="space-y-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-24 w-full" />
          ))}
        </div>
      )
    }

    if (buckets.length === 0) {
      return (
        <EmptyState
          icon={HardDrive}
          title="No S3 buckets"
          description="Create a bucket to see it here."
        />
      )
    }

    return (
      <div className="space-y-4">
        <div className="flex items-center gap-3">
          <HardDrive className="h-5 w-5 text-muted-foreground" />
          <h2 className="text-xl font-bold">S3 Buckets</h2>
          <Badge variant="secondary">{buckets.length}</Badge>
        </div>

        <div className="grid gap-3">
          {buckets.map((bkt) => (
            <Card
              key={bkt.name}
              className="cursor-pointer hover:bg-accent/50 transition-colors"
              onClick={() => { setSelectedBucket(bkt.name); setPrefix('') }}
            >
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3 min-w-0">
                    <HardDrive className="h-5 w-5 text-primary flex-shrink-0" />
                    <div className="min-w-0">
                      <div className="font-medium text-sm truncate">{bkt.name}</div>
                      <div className="flex items-center gap-3 text-xs text-muted-foreground mt-1">
                        <span className="flex items-center gap-1"><Globe className="h-3 w-3" />{bkt.region}</span>
                        <span className="flex items-center gap-1"><Clock className="h-3 w-3" />{formatDate(bkt.created)}</span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-4 flex-shrink-0">
                    <div className="text-right">
                      <div className="text-sm font-medium">{bkt.object_count}</div>
                      <div className="text-xs text-muted-foreground">objects</div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-medium">{formatBytes(bkt.total_size)}</div>
                      <div className="text-xs text-muted-foreground">total</div>
                    </div>
                    <div className="flex items-center gap-1.5">
                      {bkt.versioning === 'Enabled' && (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Shield className="h-3.5 w-3.5 text-emerald-500" />
                          </TooltipTrigger>
                          <TooltipContent>Versioning enabled</TooltipContent>
                        </Tooltip>
                      )}
                      {bkt.encryption === 'Enabled' && (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Lock className="h-3.5 w-3.5 text-blue-500" />
                          </TooltipTrigger>
                          <TooltipContent>Encryption enabled</TooltipContent>
                        </Tooltip>
                      )}
                      {Object.keys(bkt.tags).length > 0 && (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Tag className="h-3.5 w-3.5 text-muted-foreground" />
                          </TooltipTrigger>
                          <TooltipContent>{Object.keys(bkt.tags).length} tags</TooltipContent>
                        </Tooltip>
                      )}
                    </div>
                    <ChevronRight className="h-4 w-4 text-muted-foreground" />
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    )
  }

  // Object browser view
  return (
    <div className="space-y-4">
      {/* Header with breadcrumb */}
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" onClick={() => { setSelectedBucket(null); setPrefix('') }} className="h-8">
          <ArrowLeft className="h-4 w-4 mr-1" />
          Buckets
        </Button>
        <Separator orientation="vertical" className="h-5" />
        <button
          onClick={() => setPrefix('')}
          className="text-sm font-medium hover:text-primary transition-colors"
        >
          {selectedBucket}
        </button>
        {breadcrumbs.map((crumb, i) => (
          <span key={i} className="flex items-center gap-1">
            <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
            <button
              onClick={() => setPrefix(breadcrumbs.slice(0, i + 1).join('/') + '/')}
              className="text-sm text-muted-foreground hover:text-primary transition-colors"
            >
              {crumb}
            </button>
          </span>
        ))}
      </div>

      {/* Objects table */}
      <Card>
        <CardHeader className="p-4 pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-sm font-medium">
              {prefix ? `${prefix}` : 'Root'}
            </CardTitle>
            {objectsData && (
              <span className="text-xs text-muted-foreground">
                {objectsData.folders.length} folders, {objectsData.files.length} files
              </span>
            )}
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {loadingObjects ? (
            <div className="p-4 space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          ) : objectsData && (objectsData.folders.length > 0 || objectsData.files.length > 0) ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[45%]">Name</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Size</TableHead>
                  <TableHead>Last Modified</TableHead>
                  <TableHead className="w-[50px]" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {/* Back navigation */}
                {prefix && (
                  <TableRow className="cursor-pointer hover:bg-accent/50" onClick={navigateUp}>
                    <TableCell className="text-xs" colSpan={5}>
                      <div className="flex items-center gap-2 text-muted-foreground">
                        <ArrowLeft className="h-3.5 w-3.5" />
                        <span>..</span>
                      </div>
                    </TableCell>
                  </TableRow>
                )}

                {/* Folders */}
                {objectsData.folders.map((folder) => {
                  const folderName = folder.slice(prefix.length).replace(/\/$/, '')
                  return (
                    <TableRow
                      key={folder}
                      className="cursor-pointer hover:bg-accent/50"
                      onClick={() => navigateToFolder(folder)}
                    >
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Folder className="h-4 w-4 text-yellow-500" />
                          <span className="text-sm font-medium">{folderName}/</span>
                        </div>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">Folder</TableCell>
                      <TableCell className="text-xs text-muted-foreground">—</TableCell>
                      <TableCell className="text-xs text-muted-foreground">—</TableCell>
                      <TableCell />
                    </TableRow>
                  )
                })}

                {/* Files */}
                {objectsData.files.map((file) => {
                  const Icon = getFileIcon(file.content_type, file.name)
                  return (
                    <TableRow
                      key={file.key}
                      className="cursor-pointer hover:bg-accent/50"
                      onClick={() => openObject(selectedBucket, file.key)}
                    >
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Icon className="h-4 w-4 text-muted-foreground" />
                          <span className="text-sm">{file.name}</span>
                        </div>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">{file.content_type}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatBytes(file.size)}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatDate(file.last_modified)}</TableCell>
                      <TableCell>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <a
                              href={getS3DownloadUrl(selectedBucket, file.key)}
                              download
                              onClick={(e) => e.stopPropagation()}
                              className="inline-flex items-center justify-center h-7 w-7 rounded-md hover:bg-accent transition-colors"
                              aria-label={`Download ${file.name}`}
                            >
                              <Download className="h-3.5 w-3.5 text-muted-foreground" />
                            </a>
                          </TooltipTrigger>
                          <TooltipContent>Download</TooltipContent>
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          ) : (
            <EmptyState
              icon={Folder}
              title={prefix ? 'Empty folder' : 'Empty bucket'}
              description="No objects in this location."
            />
          )}
        </CardContent>
      </Card>

      {/* Object detail Sheet */}
      <Sheet open={!!objectDetail} onOpenChange={(open) => !open && setObjectDetail(null)}>
        <SheetContent className="sm:max-w-lg overflow-auto">
          {objectDetail && (
            <>
              <SheetHeader>
                <SheetTitle className="break-all text-base">{objectDetail.key.split('/').pop()}</SheetTitle>
                <SheetDescription className="break-all">{objectDetail.key}</SheetDescription>
              </SheetHeader>

              <Button variant="outline" size="sm" className="w-full mt-2" asChild>
                <a href={getS3DownloadUrl(objectDetail.bucket, objectDetail.key)} download>
                  <Download className="h-4 w-4 mr-2" />
                  Download ({formatBytes(objectDetail.size)})
                </a>
              </Button>

              <div className="space-y-4 mt-4">
                {/* Properties */}
                <div>
                  <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Properties</h4>
                  <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                    <div className="text-muted-foreground">Size</div>
                    <div className="font-mono">{formatBytes(objectDetail.size)}</div>

                    <div className="text-muted-foreground">Content-Type</div>
                    <div className="font-mono text-xs">{objectDetail.content_type}</div>

                    {objectDetail.content_encoding && (
                      <>
                        <div className="text-muted-foreground">Encoding</div>
                        <div className="font-mono text-xs">{objectDetail.content_encoding}</div>
                      </>
                    )}

                    <div className="text-muted-foreground">ETag</div>
                    <div className="font-mono text-xs truncate">{objectDetail.etag}</div>

                    <div className="text-muted-foreground">Last Modified</div>
                    <div>{formatDate(objectDetail.last_modified)}</div>

                    {objectDetail.version_id && (
                      <>
                        <div className="text-muted-foreground">Version ID</div>
                        <div className="font-mono text-xs truncate">{objectDetail.version_id}</div>
                      </>
                    )}
                  </div>
                </div>

                {/* User metadata */}
                {Object.keys(objectDetail.metadata).length > 0 && (
                  <>
                    <Separator />
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">User Metadata</h4>
                      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                        {Object.entries(objectDetail.metadata).map(([k, v]) => (
                          <div key={k} className="contents">
                            <div className="text-muted-foreground font-mono text-xs">{k}</div>
                            <div className="font-mono text-xs">{v}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </>
                )}

                {/* HTTP headers */}
                {Object.keys(objectDetail.preserved_headers).length > 0 && (
                  <>
                    <Separator />
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">HTTP Headers</h4>
                      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                        {Object.entries(objectDetail.preserved_headers).map(([k, v]) => (
                          <div key={k} className="contents">
                            <div className="text-muted-foreground font-mono text-xs">{k}</div>
                            <div className="font-mono text-xs">{v}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </>
                )}

                {/* Tags */}
                {Object.keys(objectDetail.tags).length > 0 && (
                  <>
                    <Separator />
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Tags</h4>
                      <div className="flex flex-wrap gap-1.5">
                        {Object.entries(objectDetail.tags).map(([k, v]) => (
                          <Badge key={k} variant="secondary" className="text-xs">
                            {k}: {v}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  </>
                )}

                {/* Raw JSON */}
                <Separator />
                <div>
                  <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Raw</h4>
                  <JsonViewer data={objectDetail} />
                </div>
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </div>
  )
}
